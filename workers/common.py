
# This is essentially the entry point for all workers
# It contains the main() which starts up:
# 1.  Sends 'wakeup' message to main scheduler
# 2.  Waits for response to 'wakeup', which includes
#     the worker node configuration
#     a.  Includes configuration details for all attached
#         sensors
# 3.  Configures itself
# 4.  Starts normal ops

#--------------------------------------------------------------------
# main() serves as the entry point for every worker that starts up.
# The configuration passed to main() from the CLI determines what
# type of worker we will become.  The possibilities are:
# 1.  database_interface
#     a.  reads/writes data from/to the database
#     b.  reads worker configuration data and sends it to the
#         requesting worker so it can self configure
# 2.  sensor_manager
#     a.  started by node manager, address of the instance is
#         passed on the CLI by the node_manager
#     b.  upon initial startup, sends message to the
#         database_interface, with its address, and requests
#         its configuration
#     c.  reads sensor data based upon configured schedule
#         and sends to the database worker for database insertion
#     d.  responds to ad hoc requests sending to the requester
#         and optionally to the database worker as well, if
#         the ad hoc request indicates that
# 3.  node_manager
#     a.  1 per physical node for sensor nodes
#     b.  when the physical node boots, one instance of the
#         node_manager is auto started and it does the following:
#         i.  Notifies the database_interface that the physical
#             node is awake
#         ii. waits for the database_interface to reply with the
#             node configuration data
#         iii.  starts up >= 1 sensor_managers based upon the
#             config and passes the appropriate configuration to
#             each sensor_manager
#     c.  The address of the physical node is configured in
#         /etc/doug/doug.conf, which is a YAML file
#--------------------------------------------------------------------

def main():
  import os
  import sys

  sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/.")
  sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")
  full_class_name = 'temperature.Thermocouple'
  class_name_parts = full_class_name.split('.')
  sensors = __import__('sensors.' + '.'.join(class_name_parts[0:len(class_name_parts)-1]))
  temperature = getattr(sensors, class_name_parts[-2])
  sensor_class = getattr(temperature, class_name_parts[-1])
  sensor_instance = sensor_class( { 'simulate': True, 'low': 25.0, 'high': 26.0} )
  print(sensor_instance.read())
  
  # test = workers
  # tc = getattr(getattr(getattr(workers, 'temperature'), 'thermocouple'), 'Test')
  # print(thermocouple.__dict__.keys())
  # tc.read()

if __name__ == "__main__":
  main()
