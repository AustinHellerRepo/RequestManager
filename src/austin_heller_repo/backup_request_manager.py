from __future__ import annotations
from typing import List, Tuple, Dict
from austin_heller_repo.kafka_manager import KafkaManager, KafkaReader
from austin_heller_repo.threading import Semaphore, TimeoutThread, BooleanReference, AsyncHandle
import uuid
import json
from datetime import datetime


class MakeReservationException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class UnencryptedRequestInstance():

	def __init__(self, *, request_uuid: str, kafka_manager: KafkaManager, alert_notification_async_handle: AsyncHandle):

		self.__request_uuid = request_uuid
		self.__kafka_manager = kafka_manager
		self.__alert_notification_async_handle = alert_notification_async_handle

		self.__request_instance_uuid = str(uuid.uuid4())
		self.__response_async_handle = None  # type: AsyncHandle

		self.__start_waiting_for_response()

	def __start_waiting_for_response(self):

		def navigate_messages_thread_method(is_cancelled: BooleanReference):
			# actually wait for initial notification to be pushed to Kafka topic
			self.__alert_notification_async_handle.get_result()

			kafka_reader = self.__kafka_manager.get_reader(
				topic_name=self.__request_uuid,
				group_name=self.__request_instance_uuid,
				is_from_beginning=True
			)
			is_result_found = False
			while not is_result_found:
				message_bytes = kafka_reader.read_message().get_result()  # type: bytes
				message_string = message_bytes.decode()
				message_json = json.loads(message_string)  # type: Dict
				if message_json["type"] == "response":
					response = message_json["data"]
					is_result_found = True
			return response

		self.__response_async_handle = AsyncHandle(
			get_result_method=navigate_messages_thread_method
		)
		self.__response_async_handle.try_wait(
			timeout_seconds=0
		)

	def try_wait_for_response(self, *, timeout_seconds: float) -> bool:

		return self.__response_async_handle.try_wait(
			timeout_seconds=timeout_seconds
		)

	def get_response(self) -> List[Dict]:

		return self.__response_async_handle.get_result()


class UnencryptedReservationInstance():

	def __init__(self, *, kafka_manager: KafkaManager, reservation_uuid: str, request_uuid: str, json_data_array: List[Dict]):

		self.__kafka_manager = kafka_manager
		self.__reservation_uuid = reservation_uuid
		self.__request_uuid = request_uuid
		self.__json_data_array = json_data_array

	def get_request_data(self) -> List[Dict]:
		return self.__json_data_array

	def submit_unencrypted_response(self, *, json_data_array: List[Dict]) -> AsyncHandle:

		kafka_writer = self.__kafka_manager.get_async_writer()

		return kafka_writer.write_message(
			topic_name=self.__request_uuid,
			message_bytes=json.dumps({
				"type": "response",
				"is_encrypted": False,
				"reservation_uuid": self.__reservation_uuid,
				"data": json_data_array
			})
		)


class NotificationInstance():

	def __init__(self, *, kafka_manager: KafkaManager, request_uuid: str, json_data_array: List[Dict]):

		self.__kafka_manager = kafka_manager
		self.__request_uuid = request_uuid
		self.__json_data_array = json_data_array

	def try_reserve_unencrypted_request(self) -> UnencryptedReservationInstance:

		# read for any other reservations

		kafka_reader = self.__kafka_manager.get_reader(
			topic_name=self.__request_uuid,
			group_name=str(uuid.uuid4()),
			is_from_beginning=True
		)

		is_already_reserved = False
		while not is_already_reserved:
			message_bytes = kafka_reader.read_message()  # type: bytes
			message_string = message_bytes.decode()
			message_json = json.loads(message_string)
			if message_json["type"] == "reservation":
				is_already_reserved = True
				break

		reservation_instance = None

		if not is_already_reserved:

			reservation_uuid = str(uuid.uuid4())

			kafka_writer = self.__kafka_manager.get_async_writer()

			# TODO add public key
			# TODO add encrypted reservation_uuid
			reservation_async_handler = kafka_writer.write_message(
				topic_name=self.__request_uuid,
				message_bytes=json.dumps({
					"type": "reservation",
					"is_encrypted": False,
					"reservation_uuid": reservation_uuid,
					"create_datetime": datetime.utcnow()
				}).encode()
			)

			# wait for my attempt to be pushed to Kafka

			reservation_async_handler.get_result()

			# ensure that first reserved message is mine

			is_first_reservation_mine = None
			while is_first_reservation_mine is None:
				message_bytes = kafka_reader.read_message()  # type: bytes
				message_string = message_bytes.decode()
				message_json = json.loads(message_string)
				if message_json["type"] == "reservation":
					# TODO decrypt reservation_uuid using private key in order to compare it
					if message_json["reservation_uuid"] == reservation_uuid:
						is_first_reservation_mine = True
					else:
						is_first_reservation_mine = False
					break

			if is_first_reservation_mine is None:
				raise MakeReservationException(f"Failed to find reservation after pushing it to topic.")
			else:

				reservation_instance = UnencryptedReservationInstance(
					kafka_manager=self.__kafka_manager,
					reservation_uuid=reservation_uuid,
					request_uuid=self.__request_uuid,
					json_data_array=self.__json_data_array
				)

		return reservation_instance


class RequestManager():

	def __init__(self, *, kafka_manager: KafkaManager):

		self.__kafka_manager = kafka_manager

	def submit_unencrypted_request(self, *, component_uuid: str, json_data_array: List[Dict]) -> UnencryptedRequestInstance:

		request_uuid = str(uuid.uuid4())

		self.__kafka_manager.add_topic(
			topic_name=request_uuid
		)

		kafka_writer = self.__kafka_manager.get_async_writer()

		# TODO include public key
		submit_request_async_handle = kafka_writer.write_message(
			topic_name=request_uuid,
			message_bytes=json.dumps({
				"type": "request",
				"is_encrypted": False,
				"create_datetime": datetime.utcnow()
			}).encode()
		)

		# TODO wait for downstream to reserve, including their public key
		# TODO encrypt data using their public key

		submit_request_async_handle.get_result()

		alert_notification_async_handle = kafka_writer.write_message(
			topic_name=component_uuid,
			message_bytes=json.dumps({
				"type": "notification",
				"is_encrypted": False,
				"request_uuid": request_uuid,
				"create_datetime": datetime.utcnow(),
				"data": json_data_array
			})
		)

		request_instance = UnencryptedRequestInstance(
			request_uuid=request_uuid,
			kafka_manager=self.__kafka_manager,
			alert_notification_async_handle=alert_notification_async_handle
		)

		return request_instance

	# TODO replace try_reserve_unencrypted_request with following method:
	def try_get_next_notification(self, *, component_uuids: List[str], timeout_seconds: float) -> NotificationInstance:

		kafka_readers = []  # type: List[KafkaReader]

		scan_uuid = str(uuid.uuid4())

		for component_uuid in component_uuids:
			kafka_reader = self.__kafka_manager.get_reader(
				topic_name=component_uuid,
				group_name=scan_uuid,
				is_from_beginning=True
			)
			kafka_readers.append(kafka_reader)

		def get_next_notification_thread_method():
			nonlocal kafka_readers


