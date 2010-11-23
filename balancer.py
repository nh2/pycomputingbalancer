#!/usr/bin/env python
import sys
from time import time

from periodic import Periodic

class Balancer(object):

	def __init__(self, n, chunksize, ping_timeout, cleanup_interval):
		self.n = n
		self.chunksize = chunksize

		self.ping_timeout = ping_timeout
		self.cleanup_interval = cleanup_interval

		self.packnum = self.n / self.chunksize + (1 if self.n % 1 > 0 else 0)

		self.undone = set([ x for x in xrange(self.packnum) ])
		self.working = set()
		self.done = set()

		self.pingtimes = {}

		self.cleanup_thread = Periodic(self.cleanup, self.cleanup_interval)

	def get_status(self):
		return "status: %d/%d/%d (undone/working/done)" % (len(self.undone), len(self.working), len(self.done))

	def get_start_job(self):
		if self.undone:
			job = self.undone.pop()
			self.working.add(job)
			self.pingtimes[job] = time()
			print "job %d started" % job
			print self.get_status()
			msg = {'work': True, 'n': self.n, 'job': job, 'chunksize': self.chunksize}
			return msg
		else:
			return {'work': False, 'shutdown': len(self.working)==0}

	def refresh_job(self, job, info={}):
		if job in self.working:
			print "received refresh ping for job %d" % job
			self.pingtimes[job] = time()
			return True
		else:
			print "ignoring unexpected refresh ping for job %d" % job
			return False

	def finish_job(self, job, success, info={}):
		try:
			self.working.remove(job)
			if success:
				self.done.add(job)
				print "job %d completed successfully" % job
			else:
				self.undone.add(job)
				print "job %d failed with error: %s" % (job, info.get('error', "unspecified"))
			print self.get_status()
			del self.pingtimes[job]
			return True
		except KeyError:
			print "got unexpected finishing notification for job %d" % job
			return False

	def clean_dead_jobs(self):
		now = time()
		dead = set()
		for job in self.pingtimes:
			if now - self.pingtimes[job] > self.ping_timeout:
				dead.add(job)
		num = len(self.working)
		self.undone.union(dead)
		self.working -= dead
		return num - len(self.working)
		# TODO: delete done jobs from pingtimes to save some memory

	def cleanup(self):
		numcleaned = self.clean_dead_jobs()
		if numcleaned:
			print "cleaned %d dead jobs" % numcleaned
			print self.get_status()

	def start(self):
		self.cleanup_thread.start()

	def stop(self):
		self.cleanup_thread.stop()


if __name__ == "__main__":

	from SimpleXMLRPCServer import SimpleXMLRPCServer

	import logging

	logging.basicConfig(
		level=logging.DEBUG,
		format='%(asctime)s %(levelname)s %(message)s',
	)

	PORT = 8000

	try:
		print 'Use Control-C to exit'

		server = SimpleXMLRPCServer(("localhost", PORT), logRequests=False)
		print "Listening on port %d..." % PORT

		balancer = Balancer(n=3292673, chunksize=1000, ping_timeout=5, cleanup_interval=5)
		server.register_instance(balancer)
		balancer.start()

		server.serve_forever()
	except KeyboardInterrupt:
		print 'Exiting'
		balancer.stop()
		sys.exit(0)
