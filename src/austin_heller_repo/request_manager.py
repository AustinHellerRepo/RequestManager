from __future__ import annotations
from typing import List, Tuple, Dict, Callable
from datetime import datetime
from austin_heller_repo.kafka_manager import KafkaManager, KafkaReader, KafkaReaderSeekIndex
from austin_heller_repo.threading import AsyncHandle, BooleanReference, ReadOnlyAsyncHandle
import uuid
import json
from abc import ABC, abstractmethod


class Message(ABC):

	def __init__(self, *, message_type: str, is_pointer: bool, process_instance_uuid: str, json_data_array: List[Dict], source_uuid: str, destination_uuid: str, parent_message_uuid: str):

		self.__message_type = message_type
		self.__is_pointer = is_pointer
		self.__process_instance_uuid = process_instance_uuid
		self.__json_data_array = json_data_array
		self.__source_uuid = source_uuid
		self.__destination_uuid = destination_uuid
		self.__parent_message_uuid = parent_message_uuid

		self.__message_uuid = str(uuid.uuid4())
		self.__create_datetime = datetime.utcnow()

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	def is_pointer(self) -> bool:
		return self.__is_pointer

	def get_process_instance_uuid(self) -> str:
		return self.__process_instance_uuid

	def get_message_type(self) -> str:
		return self.__message_type

	def get_create_datetime(self) -> datetime:
		return self.__create_datetime

	def get_json_data_array(self) -> List[Dict]:
		return self.__json_data_array

	def get_source_uuid(self) -> str:
		return self.__source_uuid

	def get_destination_uuid(self) -> str:
		return self.__destination_uuid

	def get_parent_message_uuid(self) -> str:
		return self.__parent_message_uuid

	def get_location_uuid(self) -> str:
		if self.__is_pointer:
			return self.__source_uuid
		else:
			return self.__destination_uuid

	def to_json(self) -> Dict:
		return {
			"id": self.__message_uuid,
			"type": self.__message_type,
			"pointer": self.__is_pointer,
			"process": self.__process_instance_uuid,
			"create_datetime": self.__create_datetime.strftime("%Y-%m-%d %H:%M:%S.%f"),
			"data": self.__json_data_array,
			"source": self.__source_uuid,
			"destination": self.__destination_uuid,
			"parent_id": self.__parent_message_uuid
		}

	def to_bytes(self) -> bytes:
		return json.dumps(self.to_json()).encode()

	@staticmethod
	def get_message_from_bytes(*, message_bytes: bytes) -> Message:
		message_string = message_bytes.decode()
		message_json = json.loads(message_string)
		return Message.get_message_from_json(
			message_json=message_json
		)

	@staticmethod
	def get_message_from_json(*, message_json: Dict) -> Message:

		message = Message(
			message_type=message_json["type"],
			is_pointer=message_json["pointer"],
			process_instance_uuid=message_json["process"],
			json_data_array=message_json["data"],
			source_uuid=message_json["source"],
			destination_uuid=message_json["destination"],
			parent_message_uuid=message_json["parent_id"]
		)

		message.__message_uuid = message_json["id"]

		create_datetime_string = message_json["create_datetime"]  # type: str
		create_datetime = datetime.strptime(create_datetime_string, "%Y-%m-%d %H:%M:%S.%f")
		message.__create_datetime = create_datetime

		return message

	def send(self, *, kafka_manager: KafkaManager) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> Message:
			nonlocal kafka_manager

			kafka_writer = kafka_manager.get_async_writer()

			message_bytes_async_handle = kafka_writer.write_message(
				topic_name=self.get_location_uuid(),
				message_bytes=self.to_bytes()
			)

			message_bytes_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			if not message_bytes_async_handle.try_wait(
				timeout_seconds=5
			):
				raise Exception(f"Message: send: message_bytes_async_handle")

			message_bytes = message_bytes_async_handle.get_result()

			message = None

			if not read_only_async_handle.is_cancelled():
				message = Message.get_message_from_bytes(
					message_bytes=message_bytes
				)

			return message

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class TopicScannerMatch():

	def __init__(self, *, message: Message, kafka_reader_seek_index: KafkaReaderSeekIndex):

		self.__message = message
		self.__kafka_reader_seek_index = kafka_reader_seek_index

	def get_message(self) -> Message:
		return self.__message

	def get_kafka_reader_seek_index(self) -> KafkaReaderSeekIndex:
		return self.__kafka_reader_seek_index


class TopicScanner():

	def __init__(self, *, kafka_manager: KafkaManager, topic_name: str, end_of_topic_read_timeout_seconds: float):

		self.__kafka_manager = kafka_manager
		self.__topic_name = topic_name
		self.__end_of_topic_read_timeout_seconds = end_of_topic_read_timeout_seconds

	def get_first_message_matching_criteria(self, *, is_match_method: Callable[[Message], bool], start_at_kafka_reader_seek_index: KafkaReaderSeekIndex or None) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> TopicScannerMatch:
			nonlocal is_match_method
			nonlocal start_at_kafka_reader_seek_index

			kafka_reader_async_handle = self.__kafka_manager.get_reader(
				topic_name=self.__topic_name,
				is_from_beginning=True
			)

			kafka_reader_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			kafka_reader = kafka_reader_async_handle.get_result()  # type: KafkaReader

			if start_at_kafka_reader_seek_index is not None:
				set_seek_index_async_handle = kafka_reader.set_seek_index(
					kafka_reader_seek_index=start_at_kafka_reader_seek_index
				)
				set_seek_index_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				set_seek_index_async_handle.get_result()

			topic_scanner_match = None  # type: TopicScannerMatch
			is_last_message_read = False
			while topic_scanner_match is None and not is_last_message_read and not read_only_async_handle.is_cancelled():
				read_async_handle = kafka_reader.try_read_message(
					timeout_seconds=self.__end_of_topic_read_timeout_seconds
				)
				read_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				message_bytes = read_async_handle.get_result()  # type: bytes
				if not read_only_async_handle.is_cancelled():
					if message_bytes is None:
						is_last_message_read = True
					else:
						message = Message.get_message_from_bytes(
							message_bytes=message_bytes
						)
						print(f"checking topic {self.__topic_name} message: {message.to_json()}")
						if is_match_method(message):
							kafka_reader_seek_index_async_handle = kafka_reader.get_seek_index()

							kafka_reader_seek_index_async_handle.add_parent(
								async_handle=read_only_async_handle
							)

							kafka_reader_seek_index = kafka_reader_seek_index_async_handle.get_result()

							topic_scanner_match = TopicScannerMatch(
								message=message,
								kafka_reader_seek_index=kafka_reader_seek_index
							)
							print(f"found message")
			return topic_scanner_match

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class RequestInstance():

	def __init__(self, *, kafka_manager: KafkaManager, process_instance_uuid: str, request_message: Message, notification_message: Message):

		self.__kafka_manager = kafka_manager
		self.__process_instance_uuid = process_instance_uuid
		self.__request_message = request_message
		self.__notification_message = notification_message

	def get_request_message(self) -> Message:
		return self.__request_message

	def get_notification_message(self) -> Message:
		return self.__notification_message

	def get_response_json_data_array(self, *, end_of_topic_read_timeout_seconds: float, request_message_topic_scanner_match: TopicScannerMatch) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> List[Dict]:
			nonlocal end_of_topic_read_timeout_seconds
			nonlocal request_message_topic_scanner_match

			topic_scanner = TopicScanner(
				kafka_manager=self.__kafka_manager,
				topic_name=self.__process_instance_uuid,
				end_of_topic_read_timeout_seconds=end_of_topic_read_timeout_seconds
			)

			def is_response(message: Message):
				return message.get_message_type() == "response"

			response_message_async_handle = topic_scanner.get_first_message_matching_criteria(
				is_match_method=is_response,
				start_at_kafka_reader_seek_index=request_message_topic_scanner_match.get_kafka_reader_seek_index()
			)

			response_message_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			response_message_topic_scanner_match = response_message_async_handle.get_result()  # type: TopicScannerMatch

			if not response_message_async_handle.is_cancelled():
				json_data_array = response_message_topic_scanner_match.get_message().get_json_data_array()
			else:
				json_data_array = None

			return json_data_array

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class RequestManager():

	def __init__(self, *, kafka_manager: KafkaManager):

		self.__kafka_manager = kafka_manager

		self.__process_instance_uuid = str(uuid.uuid4())

	def submit_request(self, *, destination_uuid: str, json_data_array: List[Dict]) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> RequestInstance:
			nonlocal destination_uuid
			nonlocal json_data_array

			add_topic_async_handle = self.__kafka_manager.add_topic(
				topic_name=self.__process_instance_uuid
			)

			add_topic_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			add_topic_async_handle.get_result()

			request_instance = None  # type: RequestInstance

			request_message = Message(
				message_type="request",
				is_pointer=True,
				process_instance_uuid=self.__process_instance_uuid,
				json_data_array=json_data_array,
				source_uuid=self.__process_instance_uuid,
				destination_uuid=destination_uuid,
				parent_message_uuid=None
			)

			request_async_handle = request_message.send(
				kafka_manager=self.__kafka_manager
			)

			request_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			sent_request_message = request_async_handle.get_result()  # type: Message

			if not read_only_async_handle.is_cancelled():
				notification_message = Message(
					message_type="notification",
					is_pointer=False,
					process_instance_uuid=self.__process_instance_uuid,
					json_data_array=None,
					source_uuid=self.__process_instance_uuid,
					destination_uuid=destination_uuid,
					parent_message_uuid=request_message.get_message_uuid()
				)

				notification_async_handle = notification_message.send(
					kafka_manager=self.__kafka_manager
				)

				notification_async_handle.add_parent(
					async_handle=read_only_async_handle
				)

				sent_notification_message = notification_async_handle.get_result()  # type: Message

				request_instance = RequestInstance(
					kafka_manager=self.__kafka_manager,
					process_instance_uuid=self.__process_instance_uuid,
					request_message=sent_request_message,
					notification_message=sent_notification_message
				)

			return request_instance

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class NotificationManager():

	def __init__(self, *, kafka_manager: KafkaManager, end_of_topic_read_timeout_seconds: float):

		self.__kafka_manager = kafka_manager
		self.__end_of_topic_read_timeout_seconds = end_of_topic_read_timeout_seconds

		self.__process_instance_uuid = str(uuid.uuid4())
		self.__next_kafka_reader_seek_index_per_topic_name = {}  # type: Dict[str, KafkaReaderSeekIndex]

	def get_next_available_notification_message(self, *, topic_name: str) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			nonlocal topic_name

			topic_scanner = TopicScanner(
				kafka_manager=self.__kafka_manager,
				topic_name=topic_name,
				end_of_topic_read_timeout_seconds=self.__end_of_topic_read_timeout_seconds
			)

			previous_notification_message_uuid = None  # type: str
			is_past_previous_notification_message_uuid = True
			next_available_notification_message_topic_scanner_match = None  # type: TopicScannerMatch

			while next_available_notification_message_topic_scanner_match is None and not read_only_async_handle.is_cancelled():

				is_past_previous_notification_message_uuid = previous_notification_message_uuid is None

				def is_notification_match(message: Message) -> bool:
					nonlocal is_past_previous_notification_message_uuid
					if not is_past_previous_notification_message_uuid:
						if message.get_message_uuid() == previous_notification_message_uuid:
							is_past_previous_notification_message_uuid = True
						print(f"failed to match: {message.to_json()}")
						return False
					else:
						print(f"failed to match: {message.to_json()}")
						is_notification = message.get_message_type() == "notification"
						if is_notification:
							print(f"found notification")
							return True
						else:
							print(f"failed to find notification")
							return False

				print(f"about to scan 00 {topic_name}")

				if topic_name not in self.__next_kafka_reader_seek_index_per_topic_name:
					self.__next_kafka_reader_seek_index_per_topic_name[topic_name] = None
				notification_start_at_kafka_reader_seek_index = self.__next_kafka_reader_seek_index_per_topic_name[topic_name]

				notification_async_handle = topic_scanner.get_first_message_matching_criteria(
					is_match_method=is_notification_match,
					start_at_kafka_reader_seek_index=notification_start_at_kafka_reader_seek_index
				)

				notification_async_handle.add_parent(
					async_handle=read_only_async_handle
				)

				found_notification_message_topic_scanner_match = notification_async_handle.get_result()  # type: TopicScannerMatch
				print(f"finished scan 00 {topic_name}")

				if not read_only_async_handle.is_cancelled():
					previous_notification_message_uuid = found_notification_message_topic_scanner_match.get_message().get_message_uuid()

					def is_reservation_match(message: Message) -> bool:
						nonlocal found_notification_message_topic_scanner_match
						return message.get_parent_message_uuid() == found_notification_message_topic_scanner_match.get_message().get_message_uuid() and \
							message.get_message_type() == "reservation"

					print(f"about to scan 01 {topic_name}")
					reservation_async_handle = topic_scanner.get_first_message_matching_criteria(
						is_match_method=is_reservation_match,
						start_at_kafka_reader_seek_index=found_notification_message_topic_scanner_match.get_kafka_reader_seek_index()
					)

					reservation_async_handle.add_parent(
						async_handle=read_only_async_handle
					)

					found_reservation_message_topic_scanner_match = reservation_async_handle.get_result()  # type: TopicScannerMatch
					print(f"finished scan 01 {topic_name}")

					if not read_only_async_handle.is_cancelled():
						if found_reservation_message_topic_scanner_match is None:
							next_available_notification_message_topic_scanner_match = found_notification_message_topic_scanner_match
						else:
							self.__next_kafka_reader_seek_index_per_topic_name[topic_name] = found_notification_message_topic_scanner_match.get_kafka_reader_seek_index()
			return next_available_notification_message_topic_scanner_match

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def reserve_notification_message(self, *, notification_message_topic_scanner_match: TopicScannerMatch) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> Message:
			nonlocal notification_message_topic_scanner_match

			found_reservation_message_topic_scanner_match = None  # type: TopicScannerMatch

			reservation_message = Message(
				message_type="reservation",
				is_pointer=False,
				process_instance_uuid=self.__process_instance_uuid,
				json_data_array=None,
				source_uuid=self.__process_instance_uuid,
				destination_uuid=notification_message_topic_scanner_match.get_message().get_destination_uuid(),
				parent_message_uuid=notification_message_topic_scanner_match.get_message().get_message_uuid()
			)

			reservation_message_async_handle = reservation_message.send(
				kafka_manager=self.__kafka_manager
			)

			reservation_message_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			reservation_message_async_handle.get_result()

			if not read_only_async_handle.is_cancelled():

				# check if this reservation has been inserted before any others

				def is_reservation_match(message: Message) -> bool:
					nonlocal notification_message_topic_scanner_match
					return message.get_parent_message_uuid() == notification_message_topic_scanner_match.get_message().get_message_uuid() and \
						   message.get_message_type() == "reservation"

				topic_scanner = TopicScanner(
					kafka_manager=self.__kafka_manager,
					topic_name=notification_message_topic_scanner_match.get_message().get_destination_uuid(),
					end_of_topic_read_timeout_seconds=self.__end_of_topic_read_timeout_seconds
				)

				verified_reservation_message_async_handle = topic_scanner.get_first_message_matching_criteria(
					is_match_method=is_reservation_match,
					start_at_kafka_reader_seek_index=notification_message_topic_scanner_match.get_kafka_reader_seek_index()
				)

				verified_reservation_message_async_handle.add_parent(
					async_handle=read_only_async_handle
				)

				verified_reservation_message_topic_scanner_match = verified_reservation_message_async_handle.get_result()  # type: TopicScannerMatch

				if not read_only_async_handle.is_cancelled():
					if verified_reservation_message_topic_scanner_match is None:
						raise Exception(f"Failed to send reservation message to Kafka.")
					elif verified_reservation_message_topic_scanner_match.get_message().get_message_uuid() == reservation_message.get_message_uuid():
						found_reservation_message_topic_scanner_match = verified_reservation_message_topic_scanner_match
			return found_reservation_message_topic_scanner_match.get_message()

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_notification_request(self, *, notification_message: Message) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> TopicScannerMatch:
			nonlocal notification_message

			topic_scanner = TopicScanner(
				kafka_manager=self.__kafka_manager,
				topic_name=notification_message.get_source_uuid(),
				end_of_topic_read_timeout_seconds=self.__end_of_topic_read_timeout_seconds
			)

			def is_request(message: Message) -> bool:
				nonlocal notification_message
				return message.get_message_uuid() == notification_message.get_parent_message_uuid()

			request_message_async_handle = topic_scanner.get_first_message_matching_criteria(
				is_match_method=is_request,
				start_at_kafka_reader_seek_index=None
			)

			request_message_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			found_request_message_topic_scanner_match = request_message_async_handle.get_result()  # type: TopicScannerMatch

			return found_request_message_topic_scanner_match

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def respond_to_request(self, *, request_message: Message, reservation_message: Message, json_data_array: List[Dict]) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> Message:
			nonlocal request_message
			nonlocal reservation_message
			nonlocal json_data_array

			response_message = Message(
				message_type="response",
				is_pointer=False,
				process_instance_uuid=self.__process_instance_uuid,
				json_data_array=json_data_array,
				source_uuid=reservation_message.get_destination_uuid(),
				destination_uuid=request_message.get_source_uuid(),
				parent_message_uuid=reservation_message.get_message_uuid()
			)

			send_to_destination_async_handle = response_message.send(
				kafka_manager=self.__kafka_manager
			)

			send_to_destination_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			sent_response_message = send_to_destination_async_handle.get_result()  # type: Message

			return sent_response_message

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle
