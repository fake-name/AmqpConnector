
import amqp
import logging
import threading
import queue
import time


class Connector:

	def __init__(
					self,
					host           = None,
					userid         = 'guest',
					password       = 'guest',
					virtual_host   = '/',
					consumer_queue = 'task.q',
					response_queue = 'response.q',
					exchange       = 'tasks.e',
					master         = False
				):

		self.log = logging.getLogger("Connector")

		# The master has the response and message queues swapped,
		# Because it puts messages into the consumer queue, and
		# receives them from the response queue, whereas the
		# clients all do the opposite.
		if master == True:
			consumer_queue, response_queue = response_queue, consumer_queue

		# Validity-Check args
		if not host:
			raise ValueError("You must specify a host to connect to!")

		assert consumer_queue.endswith(".q") == True
		assert response_queue.endswith(".q") == True
		assert       exchange.endswith(".e") == True

		# Move args into class variables
		self.consumer_q = consumer_queue
		self.response_q = response_queue
		self.exchange   = exchange

		# Connect to server
		self.connection = amqp.connection.Connection(host=host, userid=userid, password=password, virtual_host=virtual_host, heartbeat=30)

		# Channel and exchange setup
		self.channel = self.connection.channel()
		self.channel.basic_qos(prefetch_size=0, prefetch_count=1, a_global=True)

		self.channel.exchange_declare(self.exchange, type='direct', auto_delete=False)

		# set up consumer and response queues
		self.channel.queue_declare(self.consumer_q, auto_delete=False)
		self.channel.queue_bind(self.consumer_q, exchange=self.exchange, routing_key=self.consumer_q.split(".")[0])

		self.channel.queue_declare(self.response_q, auto_delete=False)
		self.channel.queue_bind(self.response_q, exchange=self.exchange, routing_key=self.response_q.split(".")[0])

		# set up the task and response queues.
		self.taskQueue = queue.Queue()
		self.responseQueue = queue.Queue()


		# Threading logic
		self.run = True

		# self.poll()
		self.thread = threading.Thread(target=self._poll, daemon=True)
		self.thread.start()

	def stop(self):
		'''
		Tell the AMQP interface thread to halt, and then join() on it.
		Will block until the queue has been cleanly shut down.
		'''
		self.run = False
		self.thread.join()

	def _poll(self):
		'''
		Internal function.
		Polls the AMQP interface, processing any messages received on it.
		Received messages are ack-ed, and then placed into the appropriate local queue.
		messages in the outgoing queue are transmitted.

		NOTE: Maximum throughput is 4 messages-second, limited by the internal poll-rate.
		'''
		lastHeartbeat = self.connection.last_heartbeat_received

		while self.run:
			# Kick over heartbeat
			if self.connection.last_heartbeat_received != lastHeartbeat:
				lastHeartbeat = self.connection.last_heartbeat_received
				self.log.debug("Heartbeat tick received: %s", lastHeartbeat)
			self.connection.heartbeat_tick()
			# self.connection.send_heartbeat()
			time.sleep(0.25)
			item = self.channel.basic_get(queue=self.consumer_q)
			if item:
				item.channel.basic_ack(item.delivery_info['delivery_tag'])
				self.taskQueue.put(item.body)

			try:
				put = self.responseQueue.get_nowait()
				self.log.info("Publishing message '%s'", put)

				message = amqp.basic_message.Message(body=put)

				self.channel.basic_publish(message, exchange=self.exchange, routing_key=self.response_q.split(".")[0])
			except queue.Empty:
				pass

		self.log.info("Thread Exiting")
		self.connection.close()
		self.log.info("Thread exited")

	def getMessage(self):
		'''
		Try to fetch a message from the receiving Queue.
		Returns the method if there is one, False if there is not.
		Non-Blocking.
		'''
		try:
			put = self.taskQueue.get_nowait()
			return put
		except queue.Empty:
			return None

	def putMessage(self, message):
		'''
		Place a message into the outgoing queue.
		'''
		self.responseQueue.put(message)

def test():
	import json
	import sys
	import os.path
	logging.basicConfig(level=logging.DEBUG)

	sPaths = ['./settings.json', '../settings.json']

	for sPath in sPaths:
		if not os.path.exists(sPath):
			continue
		with open(sPath, 'r') as fp:
			settings = json.load(fp)

	isMaster = len(sys.argv) > 1
	con = Connector(userid       = settings["RABBIT_LOGIN"],
					password     = settings["RABBIT_PASWD"],
					host         = settings["RABBIT_SRVER"],
					virtual_host = settings["RABBIT_VHOST"],
					master       = isMaster)

	while 1:
		try:
			time.sleep(1)
			new = con.getMessage()
			if new:
				print(new)

			if isMaster:
				con.putMessage("Oh HAI")

		except KeyboardInterrupt:
			break

	con.stop()

if __name__ == "__main__":
	test()


