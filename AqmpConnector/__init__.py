
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
					master         = False,
					synchronous    = True,
					flush_queues   = False
				):

		# The synchronous flag controls whether the connector should limit itself
		# to consuming one message at-a-time.
		# This is used for clients, which should only retreive one message, process it,
		# send a response, and only then retreive another.
		self.synchronous = synchronous

		# Number of tasks that have been retreived by this client.
		# Used for limiting the number of tasks each client will pre-download and
		# place in it's internal queues.
		self.active      = 0

		self.log = logging.getLogger("Main.Connector")

		self.log.info("Setting up AqmpConnector!")

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
		self.connection = amqp.connection.Connection(host=host, userid=userid, password=password, virtual_host=virtual_host, heartbeat=90)

		# Channel and exchange setup
		self.channel = self.connection.channel()
		self.channel.basic_qos(prefetch_size=0, prefetch_count=1, a_global=True)

		self.channel.exchange_declare(self.exchange, type='direct', auto_delete=False)

		# set up consumer and response queues
		self.channel.queue_declare(self.consumer_q, auto_delete=False)
		self.channel.queue_bind(self.consumer_q, exchange=self.exchange, routing_key=self.consumer_q.split(".")[0])

		self.channel.queue_declare(self.response_q, auto_delete=False)
		self.channel.queue_bind(self.response_q, exchange=self.exchange, routing_key=self.response_q.split(".")[0])

		# "NAK" queue, used for keeping the event loop ticking when we
		# purposefully do not want to receive messages
		self.channel.queue_declare('nak.q', auto_delete=False)
		self.channel.queue_bind('nak.q', exchange=self.exchange, routing_key="nak")


		if flush_queues:
			self.channel.queue_purge(self.consumer_q)
			self.channel.queue_purge(self.response_q)


		# set up the task and response queues.
		self.taskQueue = queue.Queue()
		self.responseQueue = queue.Queue()


		# Threading logic
		self.run = True

		# self.poll()
		self.log.info("Starting AMQP interface thread.")
		self.thread = threading.Thread(target=self._poll_proxy, daemon=True)
		self.thread.start()

	def _poll_proxy(self):
		self.log.info("AMQP interface thread started.")
		try:
			self._poll()
		except KeyboardInterrupt:
			self.log.warning("AQMP Connector thread interrupted by keyboard interrupt!")
			self._poll()

	def _poll(self):
		'''
		Internal function.
		Polls the AMQP interface, processing any messages received on it.
		Received messages are ack-ed, and then placed into the appropriate local queue.
		messages in the outgoing queue are transmitted.

		NOTE: Maximum throughput is 4 messages-second, limited by the internal poll-rate.
		'''
		lastHeartbeat = self.connection.last_heartbeat_received

		print_time = 5     # Print a status message every n seconds
		integrator = 0     # Time since last status message emitted.
		loop_delay = 0.25  # Poll interval for queues.

		while self.run:
			# Kick over heartbeat
			if self.connection.last_heartbeat_received != lastHeartbeat:
				lastHeartbeat = self.connection.last_heartbeat_received
				if integrator > print_time:
					self.log.info("Heartbeat tick received: %s", lastHeartbeat)

			self.connection.heartbeat_tick()
			time.sleep(loop_delay)
			if self.active == 0 or not self.synchronous:

				if integrator > print_time:
					self.log.info("Looping, waiting for job.")
				item = self.channel.basic_get(queue=self.consumer_q)
				if item:
					self.log.info("Received packet! Processing.")
					item.channel.basic_ack(item.delivery_info['delivery_tag'])
					self.taskQueue.put(item.body)
					self.active += 1
			else:

				if integrator > print_time:
					self.log.info("Active task running.")
				# Because the library is annoying, there is no transport activity
				# unless we *specifically* poll a queue (things like `heartbeat_tick()`
				# apparently don't actually check the rx buffer).
				# Since I don't want to consume garbate packets, we create a queue
				# specifically to consume from that's always empty
				item = self.channel.basic_get(queue='nak.q')


			while 1:
				try:
					put = self.responseQueue.get_nowait()
					self.log.info("Publishing message of len '%0.3f'K", len(put)/3)
					message = amqp.basic_message.Message(body=put)
					self.channel.basic_publish(message, exchange=self.exchange, routing_key=self.response_q.split(".")[0])
					self.active -= 1

				except queue.Empty:
					break

			# Reset the print integrator.
			if integrator > 5:
				integrator = 0
			integrator += loop_delay


		self.log.info("AMQP Thread Exiting")
		self.connection.close()
		self.log.info("AMQP Thread exited")

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



	def stop(self):
		'''
		Tell the AMQP interface thread to halt, and then join() on it.
		Will block until the queue has been cleanly shut down.
		'''
		self.log.info("Stopping AMQP interface thread.")
		self.run = False
		self.thread.join()
		self.log.info("AMQP interface thread halted.")


def test():
	import json
	import sys
	import os.path
	logging.basicConfig(level=logging.INFO)

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
					master       = isMaster,
					synchronous  = not isMaster,
					flush_queues = isMaster)

	while 1:
		try:
			# if not isMaster:
			time.sleep(1)

			new = con.getMessage()
			if new:
				print(new)
				if not isMaster:
					con.putMessage("Hi Thar!")

			if isMaster:
				con.putMessage("Oh HAI")

		except KeyboardInterrupt:
			break

	con.stop()

if __name__ == "__main__":
	test()


