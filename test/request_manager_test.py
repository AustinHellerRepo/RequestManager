import unittest
from src.austin_heller_repo.backup_request_manager import RequestManager, UnencryptedRequestInstance, UnencryptedReservationInstance
from austin_heller_repo.kafka_manager import KafkaWrapper, KafkaManager
from austin_heller_repo.threading import start_thread
import time
from datetime import datetime
import uuid


def get_default_request_manager() -> RequestManager:

	kafka_wrapper = KafkaWrapper(
		host_url="0.0.0.0",
		host_port=9092
	)

	kafka_manager = KafkaManager(
		kafka_wrapper=kafka_wrapper,
		read_polling_seconds=1.0,
		cluster_propagation_seconds=30,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30
	)

	request_manager = RequestManager(
		kafka_manager=kafka_manager
	)

	return request_manager


class RequestManagerTest(unittest.TestCase):

	def setUp(self) -> None:
		print(f"setUp: started: {datetime.utcnow()}")

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

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

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		self.assertIsNotNone(kafka_manager)

		request_manager = RequestManager(
			kafka_manager=kafka_manager
		)

		self.assertIsNotNone(request_manager)

	def test_submit_request(self):

		request_manager = get_default_request_manager()

		component_uuid = str(uuid.uuid4())

		request_instance = None  # type: UnencryptedRequestInstance

		def submit_request_thread_method():
			nonlocal request_instance

			request_instance = request_manager.submit_unencrypted_request(
				component_uuid=component_uuid,
				json_data_array=[{
					"test": True
				}, {
					"second entry": "yeah, second"
				}]
			)

		response = None  # type: List[Dict]

		def get_response_thread_method():
			nonlocal response

			response = request_instance.get_response(
				timeout_seconds=10
			)

		notification_request_uuid = None  # type: str

		def find_notification_thread_method():
			nonlocal notification_request_uuid

			raise NotImplementedError()

		reservation_instance = None  # type: UnencryptedReservationInstance

		def reserve_request_thread_method():
			nonlocal notification_request_uuid
			nonlocal reservation_instance

			reservation_instance = request_manager.try_reserve_unencrypted_request(
				request_uuid=notification_request_uuid
			)
		reservation_instance.submit_unencrypted_response(
			json_data_array=[{

			}]
		)
		def send_response_thread_method():
			nonlocal reservation_instance

			reservation_instance