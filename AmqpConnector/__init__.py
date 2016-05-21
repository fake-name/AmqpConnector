
import amqp
import socket
import traceback
import logging
import threading
import multiprocessing
import queue
import time


class Connector:

	def __init__(
					self,
					host                   = None,
					userid                 = 'guest',
					password               = 'guest',
					virtual_host           = '/',
					task_queue             = 'task.q',
					response_queue         = 'response.q',
					task_exchange          = 'tasks.e',
					task_exchange_type     = 'direct',
					response_exchange      = 'resps.e',
					response_exchange_type = 'direct',
					master                 = False,
					synchronous            = True,
					flush_queues           = False,
					heartbeat              = 60*5,
					ssl                    = None,
					poll_rate              = 0.25,
					prefetch               = 1,
					session_fetch_limit    = None,
					durable                = False,
				):

		# The synchronous flag controls whether the connector should limit itself
		# to consuming one message at-a-time.
		# This is used for clients, which should only retreive one message, process it,
		# send a response, and only then retreive another.
		self.synchronous   = synchronous
		self.poll_rate     = poll_rate
		self.prefetch      = prefetch
		self.durable       = durable

		# The session fetch limit allows control of the total number of
		# AMQP messages the instance of `Connector()` will *ever* fetch in it's
		# entire lifetime.
		# If none, there will be no limit.
		self.session_fetch_limit = session_fetch_limit
		self.session_fetched     = 0
		self.queue_fetched       = 0

		# Number of tasks that have been retreived by this client.
		# Used for limiting the number of tasks each client will pre-download and
		# place in it's internal queues.
		self.active      = 0

		self.log = logging.getLogger("Main.Connector")

		self.log.info("Setting up AqmpConnector!")


		self.log.info("Fetch limit: '%s'", self.session_fetch_limit)
		self.log.info("Comsuming from queue '{conq}', emitting responses on '{tasq}'.".format(conq = task_queue, tasq=response_queue))
		self.master = master

		# Validity-Check args
		if not host:
			raise ValueError("You must specify a host to connect to!")
		assert        task_queue.endswith(".q") == True
		assert    response_queue.endswith(".q") == True
		assert     task_exchange.endswith(".e") == True
		assert response_exchange.endswith(".e") == True

		# Move args into class variables
		self.task_q            = task_queue
		self.response_q        = response_queue
		self.task_exchange     = task_exchange
		self.response_exchange = response_exchange

		# ssl gets passed directly to `ssl.wrap_socket` if it's a dict.
		# The invocation is `ssl.wrap_socket(socket, **sslopts)`, so you
		# can pass arbitrary kwargs.
		self.sslopts    = ssl

		# Declare here to shut up pylint.
		self.connection = None
		self.channel    = None

		# Patch in the port number to the host name if it's not present.
		# This is really clumsy, but you can't explicitly specify the port
		# in the amqp library
		if not ":" in host:
			if ssl:
				host += ":5671"
			else:
				host += ":5672"


		# Shove connection parameters into class member variables, so they'll
		# hang around when needed for reconnecting.
		self.host         = host
		self.userid       = userid
		self.password     = password
		self.virtual_host = virtual_host
		self.heartbeat    = heartbeat

		self.task_exchange_type = task_exchange_type
		self.response_exchange_type = response_exchange_type

		self._connect()
		self._setupQueues()

		if flush_queues:
			self.channel.queue_purge(self.task_q)
			self.channel.queue_purge(self.response_q)


		# set up the task and response queues.
		# These need to be multiprocessing queues because
		# messages can sometimes be inserted from a different process
		# then the interface is created in.
		self.taskQueue = multiprocessing.Queue()
		self.responseQueue = multiprocessing.Queue()


		# Threading logic
		self.run = True

		# self.poll()
		self.log.info("Starting AMQP interface thread.")
		self.thread = threading.Thread(target=self._poll_proxy, daemon=True)
		self.thread.start()

	def _connect(self):

		# Connect to server
		self.connection = amqp.connection.Connection(host        =self.host,
													userid       =self.userid,
													password     =self.password,
													virtual_host =self.virtual_host,
													heartbeat    =self.heartbeat,
													ssl          =self.sslopts)

		# Channel and exchange setup
		self.channel = self.connection.channel()
		self.channel.basic_qos(
				prefetch_size  = 0,
				prefetch_count = self.prefetch,
				a_global       = False
			)

	def _setupQueues(self):

		self.channel.exchange_declare(self.task_exchange,     type=self.task_exchange_type,     auto_delete=False, durable=self.durable)
		self.channel.exchange_declare(self.response_exchange, type=self.response_exchange_type, auto_delete=False, durable=self.durable)

		# set up consumer and response queues
		if self.master:
			# Master has to declare the response queue so it can listen for responses
			self.channel.queue_declare(self.response_q, auto_delete=False, durable=self.durable)
			self.channel.queue_bind(   self.response_q, exchange=self.response_exchange, routing_key=self.response_q.split(".")[0])
			self.log.info("Binding queue {queue} to exchange {ex}.".format(queue=self.response_q, ex=self.response_exchange))

		if not self.master:
			# Clients need to declare their task queues, so the master can publish into them.
			self.channel.queue_declare(self.task_q, auto_delete=False, durable=self.durable)
			self.channel.queue_bind(   self.task_q, exchange=self.task_exchange, routing_key=self.task_q.split(".")[0])
			self.log.info("Binding queue {queue} to exchange {ex}.".format(queue=self.task_q, ex=self.task_exchange))

		# "NAK" queue, used for keeping the event loop ticking when we
		# purposefully do not want to receive messages
		# THIS IS A SHITTY WORKAROUND for keepalive issues.
		self.channel.queue_declare('nak.q', auto_delete=False, durable=self.durable)
		self.channel.queue_bind('nak.q',    exchange=self.response_exchange, routing_key="nak")


	def _poll_proxy(self):

		if not self.synchronous:
			if self.master:
				in_queue = self.response_q
			else:
				in_queue = self.task_q

			self.channel.basic_consume(queue=in_queue, callback=self._message_callback)

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

		print_time = 15              # Print a status message every n seconds
		integrator = 0               # Time since last status message emitted.
		loop_delay = self.poll_rate  # Poll interval for queues.

		# When run is false, don't halt until
		# we've flushed the outgoing items out the queue
		while self.run or self.responseQueue.qsize():

			try:
				# Kick over heartbeat
				if self.connection.last_heartbeat_received != lastHeartbeat:
					lastHeartbeat = self.connection.last_heartbeat_received
					if integrator > print_time:
						self.log.info("Heartbeat tick received: %s", lastHeartbeat)

				self.connection.heartbeat_tick()
				self.connection.send_heartbeat()
				time.sleep(loop_delay)

				if not self.synchronous:
					# Async mode works via callbacks
					# However, it doesn't have it's own thread, so we
					# have to pass exec to the connection ourselves.
					try:
						self.connection.drain_events(timeout=1)
					except socket.timeout:
						# drain_events raises socket.timeout
						# if there are no messages
						pass

				elif self.active == 0 and self.synchronous and self.run:

					if integrator > print_time:
						self.log.info("Looping, waiting for job.")
					self.active += self._processReceiving()

				else:
					if integrator > print_time:
						self.log.info("Active task running.")

				self._publishOutgoing()
				# Reset the print integrator.
				if integrator > 5:
					integrator = 0
				integrator += loop_delay
			except amqp.Connection.connection_errors:
				self.log.error("Connection dropped! Attempting to reconnect!")
				traceback.print_exc()
				try:
					self.connection.close()
				except Exception:
					self.log.error("Failed pre-emptive closing before reconnection. May not be a problem?")
					for line in traceback.format_exc().split('\n'):
						self.log.error(line)

				self._connect()
				self._setupQueues()

		self.log.info("AMQP Thread Exiting")

		# Stop the flow of new items
		# self.channel.flow(False)
		# Apparently flow is now disabled.

		# Close the connection once it's empty.
		self.channel.close()
		self.connection.close()

		self.log.info("AMQP Thread exited")

	def _message_callback(self, msg):
		self.log.info("Received packet via callback! Processing.")
		msg.channel.basic_ack(msg.delivery_info['delivery_tag'])
		self.taskQueue.put(msg.body)



	def _processReceiving(self):


		if self.master:
			in_queue = self.response_q
		else:
			in_queue = self.task_q

		ret = 0

		while True:
			# Prevent never breaking from the loop if the feeding queue is backed up.
			if ret > self.prefetch:
				break
			if self.atFetchLimit():
				break

			item = self.channel.basic_get(queue=in_queue)
			if item:
				self.log.info("Received packet from queue '{queue}'! Processing.".format(queue=in_queue))
				item.channel.basic_ack(item.delivery_info['delivery_tag'])
				self.taskQueue.put(item.body)
				ret += 1

				self.session_fetched += 1
				if self.atFetchLimit():
					self.log.info("Session fetch limit reached. Not fetching any additional content.")
			else:
				break

		if ret:
			self.log.info("Retreived %s items!", ret)
		return ret

	def _publishOutgoing(self):
			if self.master:
				out_queue = self.task_exchange
				out_key   = self.task_q.split(".")[0]
			else:
				out_queue = self.response_exchange
				out_key   = self.response_q.split(".")[0]

			while 1:
				try:
					put = self.responseQueue.get_nowait()
					# self.log.info("Publishing message of len '%0.3f'K to exchange '%s'", len(put)/1024, out_queue)
					message = amqp.basic_message.Message(body=put)
					if self.durable:
						message.properties["delivery_mode"] = 2
					self.channel.basic_publish(message, exchange=out_queue, routing_key=out_key)
					self.active -= 1

				except queue.Empty:
					break



	def atFetchLimit(self):
		'''
		Track the fetch-limit for the active session. Used to allow an instance to connect,
		fetch one (and only one) item, and then do things with the fetched item without
		having the background thread fetch and queue a bunch more items while it's working.
		'''
		if not self.session_fetch_limit:
			return False

		return self.session_fetched >= self.session_fetch_limit

	def atQueueLimit(self):
		'''
		Track the fetch-limit for the active session. Used to allow an instance to connect,
		fetch one (and only one) item, and then do things with the fetched item without
		having the background thread fetch and queue a bunch more items while it's working.
		'''
		if not self.session_fetch_limit:
			return False

		return self.queue_fetched >= self.session_fetch_limit


	def getMessage(self):
		'''
		Try to fetch a message from the receiving Queue.
		Returns the method if there is one, False if there is not.
		Non-Blocking.
		'''

		if self.atQueueLimit():
			raise ValueError("Out of fetchable items!")

		try:
			put = self.taskQueue.get_nowait()
			self.queue_fetched += 1
			return put
		except queue.Empty:
			return None

	def putMessage(self, message, synchronous=False):
		'''
		Place a message into the outgoing queue.

		if synchronous is true, this call will block until
		the items in the outgoing queue are less then the
		value of synchronous
		'''
		if synchronous:
			while self.responseQueue.qsize() > synchronous:
				time.sleep(0.1)
		self.responseQueue.put(message)



	def stop(self):
		'''
		Tell the AMQP interface thread to halt, and then join() on it.
		Will block until the queue has been cleanly shut down.
		'''
		self.log.info("Stopping AMQP interface thread.")
		self.run = False
		while self.responseQueue.qsize() > 0:
			self.log.info("%s remaining outgoing AMQP items.", self.responseQueue.qsize())
			time.sleep(1)

		self.log.info("%s remaining outgoing AMQP items.", self.responseQueue.qsize())

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


