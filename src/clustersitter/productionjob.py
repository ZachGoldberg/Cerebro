import logging
import threading
import time


class ProductionJob(object):
    def __init__(self,
                 task_configuration,
                 deployment_layout,
                 deployment_recipe,
                 recipe_options={}):

        # The config to pass to a machinesitter / tasksitter
        self.task_configuration = task_configuration
        self.name = task_configuration['name']
        self.deployment_recipe = deployment_recipe
        self.recipe_options = recipe_options

        # A mapping of SharedFateZoneObj : {'cpu': #CPU, 'mem': MB_Mem_Per_CPU}
        self.deployment_layout = deployment_layout

        #!MACHINEASSUMPTION!
        # Hack to make num_machines == num_cpu, for now.
        for zone in self.deployment_layout.keys():
            self.deployment_layout[zone]['num_machines'] = \
                self.deployment_layout[zone]['cpu']

    def get_shared_fate_zones(self):
        return self.deployment_layout.keys()

    def get_num_required_machines_in_zone(self, zone):
        """
        Return the total number of machines needed in this zone
        """
        #!MACHINEASSUMPTION!
        return self.deployment_layout[zone]['num_machines']

    def get_name(self):
        return self.task_configuration['name']

    def refill(self, state, sitter):
        while not state.job_fill:
            # 1) Assume this job has already been added to self.jobs
            # 2) Want to ensure calculator has run at least once to find out
            #    if this job already exists throughout the cluster
            logging.info("Waiting for calculator thread to kick in before "
                         "filling jobs")
            time.sleep(0.5)

        #!MACHINEASSUMPTION!
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        for zone in self.get_shared_fate_zones():
            idle_available = state.get_idle_machines_in_zone(zone)
            total_required = self.get_num_required_machines_in_zone(zone)
            idle_required = total_required - state.job_fill[self.name][zone]
            currently_spawning = state.spawning_machines[self.name][zone]
            idle_required -= currently_spawning

            # !MACHINEASSUMPTION! Ideally we're counting resources here not machines
            required_new_machine_count = (idle_required -
                                          len(idle_available))
            logging.info(
                ("Calculated job requirements for %s in %s: " % (self.name,
                                                                 zone)) +
                "Total Required: %s, Total New: %s" % (
                    idle_required,
                    required_new_machine_count))

            # For the machines we have idle now, use those immediately
            # For the others, spinup a thread to launch machines (which takes time)
            # and do the deployment

            # Now reserve part of the machine for this job
            usable_machines = []
            if required_new_machine_count == 0:
                # idle_available > idle_required, so use just as many
                # as we need
                usable_machines = idle_available[:idle_required]
            else:
                usable_machines.extend(idle_available)

            for machine in usable_machines:
                # Have the recipe deploy the job then set the callback
                # to be for the monitoredmachine to trigger the machinesitter
                # to actually start the job
                recipe = sitter.build_recipe(self.deployment_layout,
                                             machine,
                                             lambda: machine.start_task(self.name),
                                             self.recipe_options)

                # TODO - Mark this machine as no longer idle
                # so another job doesn't pick it up while we're deploying
                state.pending_recipes.add(recipe)

            if required_new_machine_count:
                spawn_thread = threading.Thread(
                    target=sitter.spawn_machines,
                    args=(zone, required_new_machine_count, self))
                spawn_thread.start()

            # This will get decremented automatically, aka idle_required will
            # become negative as machines start to come up.
            state.spawning_machines[self.name][zone] += idle_required
