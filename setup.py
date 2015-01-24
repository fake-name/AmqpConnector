
import setuptools
from distutils.core import setup
import sys
setup(
	# Application name:
	name="AmqpConnector",

	# Version number (initial):
	version="0.0.17",

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

	long_description=open("README.md").read(),

	# Dependent packages (distributions)
	install_requires=[
		"amqp",
	],
)


