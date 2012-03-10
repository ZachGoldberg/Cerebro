from distutils.core import setup

setup(name='Cerebro',
      version='1.0',
      description='A Cluster Management System',
      author='Zach Goldberg',
      author_email='zach@zachgoldberg.com',
      url='http://zachgoldberg.com/',
      data_files=[
        ('cerebro/templates',
         [
                'src/sittercommon/templates/cluster_overview.html',
                'src/sittercommon/templates/index.html',
                'src/sittercommon/templates/logs.html',
                'src/sittercommon/templates/stats.html',
                ])],
        packages=[
            'tasksitter',
        'machinesitter',
        'sittercommon',
        'machineconsole',
        'clustersitter'],
      package_dir={
        'tasksitter': 'src/tasksitter',
        'machinesitter': 'src/machinesitter',
        'sittercommon': 'src/sittercommon',
        'clustersitter': 'src/clustersitter',
        'machineconsole': 'src/machineconsole'},
      install_requires=[
        'boto',
        'dynect_client',
        'paramiko',
        'requests',
        'tenjin',
        ],
     )
