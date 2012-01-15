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
