
import setuptools
from distutils.core import setup
import sys
long_desc = '''
# AmqpConnector

This is a very minimal library intended for distributed task systems, where the
AMQP system is used for both task distribution, *and* collation of the task
results. This seems to be somewhat unusual among most AMQP libraries, where
a AMQP client is either a pure producer, a pure consumer, or includes everything
including the kitchen sink, a stove, a backup kitchen sink, and possibly
an entire house in case you don't have one to put the sink in (*cough* Celery
*cough*). In this case, each client consumes from one queue, and produces on
another.

At it's core, this basically just allows one to put byte-strings into one end,
they're passed out to the clients, and the clients then return byte-strings,
which come out the other end.

Correlation between submitted tasks and responses is entirely left up to the
responsibility of the end-user code.

Requires:

 - `amqp` library
 '''
setup(
	# Application name:
	name="AmqpConnector",

	# Version number (initial):
	version="0.0.39",

	# Application author details:
	author="Connor Wolf",
	author_email="github@imaginaryindustries.com",

	# Packages
	packages=["AmqpConnector"],

	# Include additional files into the package
	include_package_data=True,


	# Details
	url="http://pypi.python.org/pypi/AmqpConnector/",

	#
	# license="LICENSE.txt",
	description="Simple library for building a distributed task system over AMQP.",

	long_description=long_desc,

	# Dependent packages (distributions)
	install_requires=[
		"amqp",
	],
)


