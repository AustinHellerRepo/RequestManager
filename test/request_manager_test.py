import unittest
from src.austin_heller_repo.request_manager import RequestManager, RequestInstance, NotificationManager, TopicScanner, Message
from austin_heller_repo.kafka_manager import KafkaWrapper, KafkaManager, TopicNotFoundException
from austin_heller_repo.threading import start_thread, AsyncHandle
import time
from datetime import datetime
import uuid
from typing import List, Tuple, Dict, Callable


def get_default_kafka_manager() -> KafkaManager:

	kafka_wrapper = KafkaWrapper(
		host_url="0.0.0.0",
		host_port=9092
	)

	kafka_manager = KafkaManager(
		kafka_wrapper=kafka_wrapper,
		read_polling_seconds=1.0,
		is_cancelled_polling_seconds=0.1,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30
	)

	return kafka_manager


def get_default_request_manager() -> RequestManager:

	kafka_manager = get_default_kafka_manager()

	request_manager = RequestManager(
		kafka_manager=kafka_manager,
		end_of_topic_read_timeout_seconds=5
	)

	return request_manager


def get_default_notification_manager() -> NotificationManager:

	kafka_manager = get_default_kafka_manager()

	notification_manager = NotificationManager(
		kafka_manager=kafka_manager,
		end_of_topic_read_timeout_seconds=5
	)

	return notification_manager


class RequestManagerTest(unittest.TestCase):

	def setUp(self) -> None:
		print(f"setUp: started: {datetime.utcnow()}")

		kafka_manager = get_default_kafka_manager()

		print(f"setUp: initialized: {datetime.utcnow()}")

		topics = kafka_manager.get_topics()

		print(f"setUp: get_topics: {datetime.utcnow()}")

		for topic in topics:
			print(f"setUp: topic: {topic}: {datetime.utcnow()}")

			async_handle = kafka_manager.remove_topic(
				topic_name=topic
			)

			print(f"setUp: async: {topic}: {datetime.utcnow()}")

			async_handle.get_result()

			print(f"setUp: result: {topic}: {datetime.utcnow()}")

	def test_initialize(self):

		request_manager = get_default_request_manager()

		self.assertIsNotNone(request_manager)

	def test_submit_request_for_nonexistent_component_uuid(self):

		request_manager = get_default_request_manager()

		component_uuid = str(uuid.uuid4())

		request_instance_async_handle = request_manager.submit_request(
			destination_uuid=component_uuid,
			json_data_array=[{
				"test": True
			}, {
				"second entry": "yeah, second"
			}]
		)

		with self.assertRaises(TopicNotFoundException) as ex:
			self.assertTrue(request_instance_async_handle.try_wait(
				timeout_seconds=10
			))

		self.assertIn(component_uuid, str(ex.exception))

	def test_submit_request(self):

		request_manager = get_default_request_manager()

		component_uuid = str(uuid.uuid4())

		get_default_kafka_manager().add_topic(
			topic_name=component_uuid
		).get_result()

		request_instance_async_handle = request_manager.submit_request(
			destination_uuid=component_uuid,
			json_data_array=[{
				"test": True
			}, {
				"second entry": "yeah, second"
			}]
		)

		self.assertTrue(request_instance_async_handle.try_wait(
			timeout_seconds=10
		))

		request_instance = request_instance_async_handle.get_result()  # type: RequestInstance

		self.assertIsNotNone(request_instance)
		self.assertIsNotNone(request_instance.get_request_message())
		self.assertIsNotNone(request_instance.get_notification_message())

		print(f"request_instance.get_request_message(): {request_instance.get_request_message()}")
		print(f"request_instance.get_request_message().to_json(): {request_instance.get_request_message().to_json()}")
		print(f"request_instance.get_notification_message(): {request_instance.get_notification_message()}")
		print(f"request_instance.get_notification_message().to_json(): {request_instance.get_notification_message().to_json()}")

		# wait for response as the response is being generated

		notification_manager = get_default_notification_manager()

		next_available_notification_message_async_handle = notification_manager.get_next_available_notification_message(
			topic_name=component_uuid
		)

		start_time = datetime.utcnow()
		self.assertTrue(next_available_notification_message_async_handle.try_wait(
			timeout_seconds=30
		))
		end_time = datetime.utcnow()
		print(f"time to get next available notification message: {(end_time - start_time).total_seconds()} seconds")

		next_available_notification_message = next_available_notification_message_async_handle.get_result()  # type: Message

		self.assertIsNotNone(next_available_notification_message)

		notification_reservation_message_async_handle = notification_manager.reserve_notification_message(
			notification_message=next_available_notification_message
		)

		self.assertTrue(notification_reservation_message_async_handle.try_wait(
			timeout_seconds=10
		))

		notification_reservation_message = notification_reservation_message_async_handle.get_result()  # type: Message

		self.assertIsNotNone(notification_reservation_message)

		notification_request_message_async_handle = notification_manager.get_notification_request(
			notification_message=next_available_notification_message
		)

		self.assertTrue(notification_request_message_async_handle.try_wait(
			timeout_seconds=10
		))

		notification_request_message = notification_request_message_async_handle.get_result()  # type: Message

		self.assertIsNotNone(notification_request_message)

		expected_response_json_data_array = [
			{ "test": True },
			{ "other": {
				"hello": "world"
			}}
		]

		response_message_async_handle = notification_manager.respond_to_request(
			request_message=notification_request_message,
			reservation_message=notification_reservation_message,
			json_data_array=expected_response_json_data_array
		)

		self.assertTrue(response_message_async_handle.try_wait(
			timeout_seconds=10
		))

		response_message = response_message_async_handle.get_result()  # type: Message

		self.assertIsNotNone(response_message)

		# grab the response since it should now exist

		response_json_data_array_async_handle = request_instance.get_response_json_data_array()

		self.assertTrue(response_json_data_array_async_handle.try_wait(
			timeout_seconds=10
		))

		response_json_data_array = response_json_data_array_async_handle.get_result()  # type: List[Dict]

		self.assertEqual(expected_response_json_data_array, response_json_data_array)

	def test_topic_scanner(self):

		kafka_manager = get_default_kafka_manager()

		first_topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=first_topic_name
		).get_result()

		second_topic_name = str(uuid.uuid4())

		self.assertNotEqual(first_topic_name, second_topic_name)

		kafka_manager.add_topic(
			topic_name=second_topic_name
		).get_result()

		first_kafka_writer = kafka_manager.get_async_writer()

		first_kafka_writer.write_message(
			topic_name=first_topic_name,
			message_bytes=Message(
				message_type="type 1",
				is_pointer=False,
				process_instance_uuid=str(uuid.uuid4()),
				json_data_array=[
					{ "test": True }
				],
				source_uuid=str(uuid.uuid4()),
				destination_uuid=str(uuid.uuid4()),
				parent_message_uuid=None
			).to_bytes()
		).get_result()

		second_kafka_writer = kafka_manager.get_async_writer()

		second_kafka_writer.write_message(
			topic_name=second_topic_name,
			message_bytes=Message(
				message_type="type 2",
				is_pointer=False,
				process_instance_uuid=str(uuid.uuid4()),
				json_data_array=[
					{ "another": "one" }
				],
				source_uuid=str(uuid.uuid4()),
				destination_uuid=str(uuid.uuid4()),
				parent_message_uuid=None
			).to_bytes()
		).get_result()

		topic_scanner_0 = TopicScanner(
			kafka_manager=kafka_manager,
			topic_name=first_topic_name
		)

		def find_first(message: Message) -> bool:
			print(message.to_json())
			return False

		topic_scanner_0.get_first_message_matching_criteria(
			is_match_method=find_first
		).try_wait(
			timeout_seconds=5
		)

		topic_scanner_1 = TopicScanner(
			kafka_manager=kafka_manager,
			topic_name=second_topic_name
		)

		topic_scanner_1.get_first_message_matching_criteria(
			is_match_method=find_first
		).try_wait(
			timeout_seconds=5
		)

	# TODO test multiple submission from the same RequestManager
