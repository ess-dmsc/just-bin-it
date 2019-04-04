# Scanning for V20

## Start just-bin-it

```
>> cd just-bin-it
>> python main.py -b localhost -t hist_commands
```

## Edit v20_scan.py

The following variables define the scanning parameters:

* POSITIONS_1 = positions that motor 1 must go through
* POSITIONS_2 = positions that motor 2 must go through
* MOTOR_PREFIX = the PV prefix for the motors
* MOTOR_PV_1 = the PV for motor 1
* MOTOR_PV_2 = the PV for motor 2
* KAFKA_ADDRESS = the Kafka broker address
* JUST_BIN_IT_COMMAND_TOPIC = the topic to send commands to just-bin-it on
* EVENT_TOPIC = the topic containing the neutron events
* HISTOGRAM_TOPIC = the topic which just-bin-it will write its results
* COUNT_TIME_SECS = how long to collect data for at each point

## Run the scan

```
>> python v20_scan/main.py
```

After some time the results will be printed to screen, for example:

```
[642566.0, 640227.0, 675969.0, 640232.0, 663007.0, 668894.0]
[678745.0, 676204.0, 374401.0, 653557.0, 663588.0, 657555.0]
[675756.0, 674421.0, 681432.0, 676188.0, 668027.0, 680429.0]
[673397.0, 676081.0, 680647.0, 691626.0, 683068.0, 678130.0]
[668117.0, 641827.0, 675532.0, 672186.0, 574141.0, 680066.0]
[682980.0, 672310.0, 673621.0, 666988.0, 679392.0, 643590.0]
```

Horizontal (left to right) is motor 2 positions
Vertical (top to bottom) is motor 1 positions
