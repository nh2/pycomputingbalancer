#!/usr/bin/env python
import sys, os
import signal

from periodic import Periodic

class Job(object):
	def __init__(self, proxy, num, ping_interval, info):
		self.proxy = proxy
		self.num = num
		self.ping_thread = Periodic(self.ping_refresh, ping_interval, delay=ping_interval)
		self.info = info
		
	def ping_refresh(self):
		print "pinging for job %d" % self.num
		self.proxy.refresh_job(self.num)

	def start(self, work, msg):
		print "job %d starting" % self.num
		self.ping_thread.start()

		try:
			work(self.num, msg)
			print "job %d done" % self.num
			self.finish(success=True)
		except Exception, e:
			print "job %d failed" % self.num
			self.finish(success=False, info={'error': repr(e)})
		finally:
			self.ping_thread.stop()

	def finish(self, success, info={}):
		info.update(self.info)
		self.proxy.finish_job(self.num, success, info)

	def abort(self):
		self.ping_thread.stop()
		self.finish(success=False, info={'error': "aborted manually"})


class BalancerClient(Periodic):
	def __init__(self, proxy, ping_interval):
		self.proxy = proxy
		self.ping_interval = ping_interval
		self.info = {} # TODO: Collect some default info
		self.job = None
		super(BalancerClient, self).__init__()

	def action(self):
		msg = self.proxy.get_start_job()
		if msg['work']:
			self.job = Job(self.proxy, msg['job'], self.ping_interval, self.info)
			self.job.start(self.work, msg)
		else:
			if msg['shutdown']:
				print "Server asked to shutdown"
				self._stopevent.set()

	def stop(self, *args):
		self._stopevent.set()
		if self.job:
			self.job.abort()
		super(BalancerClient, self).stop(*args)

	def work(self, job, msg):
		raise NotImplementedError("Implement work() in a subclass or override action() for more control.")


from subprocess import Popen

class PopenBalancerClient(BalancerClient):
	def __init__(self, command_gen, *args):
		self.popen = None
		self.command_gen = command_gen
		super(PopenBalancerClient, self).__init__(*args)

	def work(self, jobnum, msg):
		cmd = self.command_gen(jobnum, msg)
		print "executing command %s" % cmd
		# Note that there is a small race condition if the Popen receives SIGINT, say, before the lambda's execution
		# This is almost impossible to fix in Python's current implementation (see http://bugs.python.org/issue1975)
		self.popen = Popen(cmd, preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
		self.popen.communicate()
		self.popen = None
	
	def stop(self, *args):
		self._stopevent.set()
		if self.popen:
			self.popen.terminate()
		super(PopenBalancerClient, self).stop(*args)


""" USAGE EXAMPLE:

	import xmlrpclib

	def command_gen(jobnum, msg):
		return ["echo", "1"]
	client = PopenBalancerClient(command_gen, xmlrpclib.ServerProxy("http://localhost:8000/"), 1)

	client.start()
	signal.signal(signal.SIGINT, lambda *args: client.stop())

	while client.is_alive():
		client.join(0.1)

"""
