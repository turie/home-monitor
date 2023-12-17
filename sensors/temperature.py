
import random
# Each class should support simulated readings
# the configuration of which gets passed in
# the cfg sent during initial setup
class Thermocouple(object):
  def __init__(self, cfg):
    self.cfg = cfg


  def read(self):
    if self.cfg['simulate']:
      return round(random.uniform(self.cfg['low'], self.cfg['high']),2)
    # Returns single value specific to sensor type
    # in the case of thermocouple, GPIO pin value
    # converted to temperature in F
