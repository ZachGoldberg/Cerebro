from setuptools import setup

setup(name='cerebrod',
      version='1.2.8',
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
        'clusterconsole',
        'clustersitter',
        'clustersitter.providers',
        'machineconsole',
        'machinesitter',
        'sittercommon',
        'sittercommon.utils',
        'tasksitter',
        ],
      package_dir={
        'clusterconsole': 'src/clusterconsole',
        'clustersitter': 'src/clustersitter',
        'machineconsole': 'src/machineconsole',
        'machinesitter': 'src/machinesitter',
        'sittercommon': 'src/sittercommon',
        'tasksitter': 'src/tasksitter',
        },
      entry_points={
        'console_scripts': [
            'cerebro = sittercommon.utils.main:main',
            'cerebrod = clustersitter.main:main',
            'clustersitter = clustersitter.main:main',
            'machineconsole = machineconsole.main:main',
            'clusterconsole = clusterconsole.main:main',
            'machinesitter = machinesitter.main:main',
            'tasksitter = tasksitter.main:main',
            ]
        },
      install_requires=[
        'boto',
        'dynect_client',
        'paramiko',
        'pycrypto',
        'requests',
        'simplejson',
        'tenjin',
        ],
     )
