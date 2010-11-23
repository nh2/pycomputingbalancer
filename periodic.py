import threading
import collections

class Periodic(threading.Thread):

	def __init__(self, action=None, sleep_period=None, delay=None, *args):
		self._action = action
		self._stopevent = threading.Event()
		self._sleep_period = sleep_period
		self._delay = delay
		if action and not isinstance(action, collections.Callable):
			raise TypeError('action must be callable')
		super(Periodic, self).__init__(*args)

	def run(self):
		if self._delay:
			self._stopevent.wait(self._delay)
		while not self._stopevent.is_set():
			self.action()
			if self._sleep_period:
				self._stopevent.wait(self._sleep_period)

	def stop(self, timeout=None):
		self._stopevent.set()
		super(Periodic, self).join(timeout)

	def action(self):
		if self._action:
			self._action()
