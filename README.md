# disexec
Simple distributed executor

This project aims to provide a simple yet powerful solution for executing 
similar tasks across multiple computers and threads, for example simulations and
other computationally intensive processess.

This project splits the functionality into one (or several) producer and multiple
consumers.

## `collector.py`
Is the producer logic, based on the configuration file, it will push tasks in JSON
format into a RabbitMQ queue; each task can return data, such results will be pushed 
into a separate queue for further processing.

## `worker.py`
The consumer logic, this process spawns threads which will connect to the specified 
queue, generate a Task object from the data and push the result of the execution.


# How to run
* Install RabbitMQ
In Debian/Ubuntu: 
	`apt install rabbitmq-server`
* Configure instance in `disexec.config`
* Run `coordinator.py` on the machine to generate all the tasks and push them
  into the queue

### Workers
For each machine that will act as a worker, just clone this repository and 
modify the `disexec.config` file accordingly to the resources of that machine

# Configuration  
### [general]
+ queue_name 
+ result_queue_name 

### [coordinator]
+ queue_url 
+ taskcreator
+ csvfile

### [task]
+ command
+ arguments
+ external_folder

### [worker]
+ cores
+ queue_url
