from setuptools import setup

setup(name='cerebrod',
      version='1.1.13',
      description='A Cluster Management System',
      author='Zach Goldberg',
      author_email='zach@zachgoldberg.com',
      url='http://zachgoldberg.com/',
      download_url='https://github.com/ZachGoldberg/Cerebro',
      zip_safe=True,
      package_data={
        '': ['templates/*.html', 'sittercommon/templates/*.html'],
         },
      packages=[
        'clustersitter',
        'clustersitter.providers',
        'machineconsole',
        'machinesitter',
        'sittercommon',
        'sittercommon.utils',
        'tasksitter',
        ],
      package_dir={
        'tasksitter': 'src/tasksitter',
        'machinesitter': 'src/machinesitter',
        'sittercommon': 'src/sittercommon',
        'clustersitter': 'src/clustersitter',
        'machineconsole': 'src/machineconsole'
        },
      entry_points={
        'console_scripts': [
            'cerebro = sittercommon.utils.main:main',
            'cerebrod = clustersitter.main:main',
            'clustersitter = clustersitter.main:main',
            'machineconsole = machineconsole.main:main',
            'machinesitter = machinesitter.main:main',
            'tasksitter = tasksitter.main:main',
            ]
        },
      install_requires=[
        'boto',
        'dynect_client',
        'paramiko',
        'requests',
        'tenjin',
        ],
     )
