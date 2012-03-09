Welcome to Cerebro!
===================

Cerebro is a job deployment and monitoring system.    Cerebro fills the gap of having some code and deploying and managing it in a cloud environment.  The simplest usecase is taking a code package and deploying it on multiple machines in ec2 with monitoring and supervising on each process.

Cerebro is a process **supervisor**,   **deployment** and **management** solution.  

Basic Feature List
--------------------

 * Monitor an individual process on a cloud hosted VM
 * Reboot the process if necessary
 * Define constraints that the process cannot pass, or it'll be rebooted
 * Provide log access via an HTTP interface
 * Monitor and manage multiple of these processes per machine
 * Monitor many machines across a cluster
 * Deploy new machines, including process harness and job code
 * Accepts python classes to define how to deploy a process or job
 * Accept a job configuration via an HTTP API which defines how many of 
   each process to deploy in which datacenters on which cloud providers
 * The ability to autodetect if a machine goes bad / disappears, decomission it
   and spin up an identcal replacement and redeploy to it, without any admin intervention.
 * Provides an HTML interface at the cluster level which gives you:
   - The ability to update jobs in place
   - An overview of what jobs are running on what machines, and where
   - Links to the STDOUT/STDERR of every process in every job on every machine, 
     across the cluster.
   - Basic machine vitals for all machines, including ram/cpu usage per process
     and total machine utilization.

Workflow
---------

A basic workflow for using Cerebro, start to finish, is as follows in 10 easy steps:

 0. Bundle your software into an easily deployable package (Using python buildout, for example)
 1. Write a python "deployment class" (see docs below) for your package (short, maybe 30 lines of code)
 2. Write a system-deployment configuration file which defines how many machines you want your code to run
    on, what your credentials are for various cloud providers, dns provider etc.
 3. Spinup a clustersitter on a cloud node (following simple deployment steps)
 4. Run the "job_update_cfg" commnd and pass it your configuration and the location of your clustersitter
 5. Look at the HTML UI, see things happen and find the provided DNS names for your machines
 6. Get more customers, increase load, need more machines
 7. Update the config file to require more machines
 8. Again run job_update_cfg with your config file
 9. Watch cerebro spin up more machines, and load to go back to acceptable levels

Under the Hood
----------------

Cerebro is made up of three parts: Task Sitter a Machine Sitter and a Cluster Sitter

### Task Sitter
Task Sitter -- A harness to manage an arbitrary task or process.

Goal: Instead of thinking about how many machines you need to run a process on
the task sitter's goal is to force the admin to think instead in terms of CPU
and RAM, an to plan how much of each resource a process should use ahead of time.

The Task Sitter's job is to enforce the limits that the admin thinks a process
should obey.  It can handle the cases where a process disobeys these limits.

Together with a machine sitter a machine can be completely managed to run
various tasks efficiently within the resource constraints of the machine.

With a cluster sitter an admin can define how many CPUs and how much RAM a particular
task can use and it can go to machines, look for available CPU and RAM where
the process fits and slot it in there.

Task Sitter Details

 * Define constraints
    * Should always be alive? (--ensure-alive)
    * Fixed % of a CPU (--cpu)
    * Fixed MB of RAM (--mem)
    * Fixed lifetime (--time-limit)

 * Define runtime metadata
    * User ID (--uid)
    * Should the proc be restarted on violation? (--restart)
    * Maximum # of reboots (--max-restarts)
    * stdout / stderr directories (--std**-location)

 * Monitoring
    * HTTP Based Monitor (--http-monitoring, --http-monitoring-port)

### Machine Sitter

Machine Sitter Details

  * Monitor a set of TaskSitters
  * Reboot TaskSitters if they fail (should never happen)
  * Provide an API to add new tasks and start/stop tasks on a machine
  * Provides central log access for all tasks

### Cluster Sitter

Cluster Sitter Details

  * Monitors a set of MachineSitters in a cluster
  * Accepts 'Jobs' which define how many cpus/memory a particular task needs,
 finds or creates machines (and deploys machinesitters if necessary) and then
 activates the "Jobs" as tasks on each machine
  * Provides a web UI to see where all your tasks are.
  * Pulls in data and aggregates it from the cluster, to see task CPU
 usage, task rebooting behavior, machine performance data etc.
  * Provides an abstract "DeploymentRecipe" class that you can fill out
 to have the clustersitter actually deploy your jobs automagically.
  * Presently knows how to spinup/teardown AWS instances, though implementing
 other cloud providers should be pretty straightforward as there is a
 pretty minimal interface to the 'providers'.
  * Assigns DNS names (presently only using the Dynect API) to machines
    so they can automagically go live (see more in the 'How to do DNS' section)
  * Decomissions machines if they fail, will spin up new ones as replacements.
  * Supports the notion of linked jobs: When job A is linked to job B job A will be placed on
    every and only the machines job A is placed on.  Job B will also be
    rebooted whenever job A is updated.
  * Keeps track of how many idle machiens you own, and can decomission them
    to keep to a predefined limit.


### Providers
 * Cerebro uses an interface for talking to both a cloud provider and a DNS provider
 * Presently only AWS/EC2 is implemented as a cloud-VM provider and Dynect is the only DNS providr
 * The interface is sufficiently minimal (aka create_instances() or dns_add_record()/dns_delete_record())
    that it should be very simple to expand to other providers (linode, rackspace etc.)

Configuration
--------------

Cerebro Configuration File:
 # See settings.py

Example Job Configuration Format

    {
        "dns_basename": "redis.startup.com",
        "deployment_recipe": "mystartup.recipes.deploy",
        "deployment_layout": {
            "aws-us-west-2a": {
                "mem": 500,
                "cpu": 1
            },
            "aws-us-east-1b": {
                "mem": 50,
                "cpu": 10
            }
        },
        "recipe_options": {
            # Passed as a dictionary to your jobs
            "release_dir": "/opt/startup/releases/"
        },
        "persistent": true,
        "task_configuration":
            {
                # Tasksitter configuration. 
                "allow_exit": false,
                "name": "Portal Server",
                "command": "/opt/wifast/run_wsgi",
                "auto_start": false,
                "ensure_alive": true,
                "max_restarts": -1,
                "restart": true,
                "uid": 0,
		"cpu": .5, # allow this job to use 50% of CPU
		"mem": 1200, # Allow this job to use 1.2GB of RAM
            }
    },


Deploymet Recipe Interface

    def run_deploy(options):
        # API?
        logger.*()

How to do DNS 

  *  In the job configuration format there is a field called "dns_basename".
  *  This should be set to something like "myjobname.mydomain.com" e.g. "redis.startup.com"
  *  Cerebro will then add two new records underneath that name for each machine.  It will

  1. Create #.PROVIDER_REGION.basename as a A record to the machine
  2. Add another CNAME to PROVIDER_REGION.basename to #.PROVIDER_REGION etc.

  You should manually setup, e.g. "redis.startup.com" to be a cname to all of the
  PROVIDER_REGION.redis.startup.com.  A complete DNS layout looks as follows:

  startup.com
  redis.startup.com (Admin Created)
     -> CNAME aws-us-west-1.redis.startup.com (Admin Created)
     -> CNAME aws-us-east-1.redis.startup.com (Admin Created)

  aws-us-west-1.redis.startup.com (Admin Created)
     -> A 45.67.20.106 (Cerebro Created)
     -> A 45.67.20.105 (Cerebro Created)
 
  0.aws-uswest-1.redis.startup.com (Cerebro Created)
     -> A 45.67.20.106 (Cerebro Created)
  1.aws-uswest-1.redis.startup.com (Cerebro Created)
     -> A 45.67.20.105 (Cerebro Created)

  aws-us-east-1.redis.startup.com (Admin Created)
     -> A 12.67.20.106 (Cerebro Created)
 
  0.aws-us-east-1.redis.startup.com (Cerebro Created)
     -> A 12.67.20.106 (Cerebro Created)


  So, if you point your servers to redis.startup.com they should get
    If your using global load balancing, a cname to one of aws-us-west-1.redis.startup.com or 
      aws-us-east-1.redis.startup.com based on the callers location
    or both CNAMEs
 
    The cname returns an A record for each machine of that type.
 
  e.g. redis.startup.com -> aws-us-west-1.redis.startup.com -> 12.67.20.106
