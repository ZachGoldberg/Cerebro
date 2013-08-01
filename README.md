(Most everything on this page is a work in progress.  Feel free to send any feedback directly to me, zach@zachgoldberg.com or file bugs on [github](https://github.com/ZachGoldberg/Cerebro/issues)).

## Overview

Cerebro simplifies and automates some of the most common system administration/ops headaches: building and scaling a cluster of virtual instances in the cloud, and covering common 'problematic scenarios' with the goal of minimizing infrastructure related 3:00 AM headaches.  For example, deploy a cluster of 4 MongoDB nodes with the push of a button, and if, 3 weeks later, one of them somehow disappears on a Saturday at 3:00 AM when all your sysadmins are out on the town finishing their last round of drinks before last call, a new instance will automatically be provisioned, Mongo deployed to the machine and booted, all without needing to force anybody to sober up and get to a terminal.

## Project Status
Cerebro has mostly been developed by [me](https://github.com/ZachGoldberg) and with some help and brainstorming from [Ryan Borgeouix](https://github.com/BlueDragonX).  Most of the features described in the overview are at least partially implemented.  It is also in use in at least one production environment and a number of development/testing environment.  That said, it is sorely lacking in documentation and is probably not yet as easy as it will be to get a fully working stack up.  

**Our goal right now is to get a few more people using the system and contributing in one way or another.**  Patches, forks, bug reports. etc are all more than welcome on the project's [github](https://github.com/ZachGoldberg/Cerebro) page, or you can contact me directly at zach@zachgoldberg.com with any issues you may be having.

## Comparison to other tools

**[Puppet](http://puppetlabs.com/)** and **[Chef](http://www.opscode.com/chef/)**
Puppet's **[self description](https://puppetlabs.com/puppet/puppet-enterprise/)**_Puppet Enterprise is IT automation software that gives system administrators the power to easily **automate repetitive tasks, quickly deploy critical applications, and proactively manage infrastructure**, on-premises or in the cloud._  

**Chef**'s [self description](http://www.opscode.com/chef/#which-chef\): _Chef is an automation platform that transforms infrastructure into code_ (find me a better description... ).  Anyway, that means something along the lines of writing ruby to describe your infrastructure, and then it gives you tools to deploy that infrastructure.  If a machine that you've deployed goes down, an admin will have to get a page (from another system, not Chef, maybe a Nagios or a NewRelic) and spinup a new one manually.

Cerebro's focus is not on automating 'repetitive' tasks.  Cerebro focuses on automating the random problematic tasks that wake you up in the morning as well as one time deployment tasks.  Processes that have bloated too much, instances that fall over etc.  

The key distinction here is that Cerebro focuses a lot on your code as a running entity in the cloud.  That is, where is it, how many instances of it are there, is it alive, is it using too much RAM, etc.  All of the things you usually monitor with Nagios etc.  Chef and puppet tackle problems earlier on in the production lifecycle, namely in the machine setup phase -- ensuring code and configuration is deployed appropriately etc.  Many deployments are very sophisticated and will need more automation in the code deployment phase than Cerebro provides. in which case Puppet and Chef may be better solutions.  There may even be a world in which both tools are used in the same stack -- Cerebro to manage machines and supervise processes and puppet/chef to do deployments though this is not actively supported at present.

**[MCollective](https://puppetlabs.com/mcollective/introduction/)**, **[Capistrano](http://www.capistranorb.com/)**, **[Func](https://fedorahosted.org/func/)** and **[Fabric](http://docs.fabfile.org/en/1.6/)**
These are self described _remote server automation and deployment tools_.  Effectively, managing servers that are already part of your cluster with ssh or similar.  Similar to puppet and chef, the core focus is on an earlier part of the sysadmin/ops lifecycle than Cerebro.  That said, Cerebro does have a remote automation step, during which it would be perfectly appropriate to invoke your fabric/chef/func/mcollective etc. scripts to setup a machine before the Cerebro process supervisor initiates and manages your jobs.


## Current Capabilities

 * Monitor an individual process on a cloud hosted VM
 * Reboot the process when certain conditions (e.g. using too much RAM) are met/exceeded
 * Provide STDUOT/STDERR as well as process metadata and statistics via HTML (human readable) and JSON (machine readable) HTTP APIs.  No more SSHing into a machine and manually tailing log files.
 * Monitor and manage multiple "jobs" or processes per machine
 * Monitor many machines across a cluster
 * Provision new machines in various cloud environments (currently only EC2 is supported, but plugins for rackspace and others are in the works)
 * Customizable job deployment via python with a [Fabric](http://docs.fabfile.org/en/1.6/)-like API.  (i.e. codify the deployment of your custom processes so cerebro can do it in a repeatable manner).
 * Cluster management via a JSON API, with command line tools and wrappers
 * The ability to auto-detect if a machine goes bad / disappears, decommission it
   and spin up an identical replacement and redeploy to it, without any admin intervention.
 * Provides an HTML interface at the cluster level which has:
   - The ability to update jobs in place (aka update code)
   - An overview of what jobs are running on what machines, and where
   - Links to the STDOUT/STDERR of every process in every job on every machine, 
     across the cluster.
   - Basic machine vitals for all machines, including ram/cpu usage per process
     and total machine utilization.

## Getting Started in 6 steps

 0. Bundle your software into an easily deployable package (Using python buildout, or an egg, or a WAR file, or a .deb etc.  If you don't already do this it's not a bad idea to start now).  
 1. Write a python "deployment class" (see docs below) for your package 
 2. Write a system-deployment configuration file which defines how many machines you want your code to run on and provides credentials to needed cloud APIs (VM provider, DNS provider etc.)
 3. Easy install cerebrod on a management server and start the daemon with "cerebrod" (see cerebrod --help for more).
 4. Easy install cerebrod on your development machine and run "cerebro updatejobcfg" on your local machine and pass it your configuration and the location of your clustersitter you've created in step 3.
 5. Checkout the cerebro web UI (http://managementserver:30000/overview) and watch things happen.  You'll find the generated DNS names for your machines and lots more info there.

And, if all goes well and you need to scale up, that's a simple 2 step process:
 0. Update your job config file to require more machines. 
 1. Run "updatejobcfg" with your updated config file

## Under the Hood

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

## Configuration
-------------

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
                "command": "/opt/code/run_portal_server",
                "auto_start": false,
                "ensure_alive": true,
                "max_restarts": -1,
                "restart": true,
                "uid": 0,
		"cpu": .5, # allow this job to use 50% of CPU
		"mem": 1200, # Allow this job to use 1.2GB of RAM
            }
    },


### Deployment Recipe Interface

    def run_deploy(options):
        # API?  Kickoff your chef recipe?  TODO: More work and structure is needed here.
        logger.*()

### DNS Setup
(This is all a bit complex right now, some simplification and optionality is needed)

  *  In the job configuration format there is a field called "dns_basename"
  *  This should be set to something like "myjobname.mydomain.com" e.g. "redis.startup.com"
  *  Cerebro will then add two new records underneath that name for each machine.  It will

      1. Create #.PROVIDER_REGION.basename as a A record to the machine
      2. Add another CNAME to PROVIDER_REGION.basename to #.PROVIDER_REGION etc.

  * You should manually setup, e.g. "redis.startup.com" to be a cname to all of the PROVIDER_REGION.redis.startup.com.  A complete DNS layout looks as follows

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


So, if you point your servers to redis.startup.com they should get either 

  1. If your using global load balancing, a cname to one of aws-us-west-1.redis.startup.com or 
      aws-us-east-1.redis.startup.com based on the callers location
  2. or both CNAMEs

The cname returns an A record for each machine of that type.  e.g. redis.startup.com -> aws-us-west-1.redis.startup.com -> 12.67.20.106

## Security
I've had a few questions on Cerebro's security model.  Namely, that there is none.   This is for two reasons: time, and it's not immediately obvious to me that one is required.  Your cloud should, in an ideal world, be completely firewalled off from the outside world.  All of cerebro's management is done via HTTP connections on non-standard ports which should be accessible only within your firewalled cloud or VPC.  To manage my machines within this environment I usually poke a hole or two with a reverse SSH port forward, or simply VPN beyond the firewall.  This isn't a perfect scenario, anybody within your cloud can do some bad things, but it seems 'good enough' until somebody cares to beef up the internal security model.
