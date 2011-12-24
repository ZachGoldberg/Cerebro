from distutils.core import setup

setup(name='TaskSitter',
      version='1.0',
      description='A Process/Task Monitor',
      author='Zach Goldberg',
      author_email='zach@zachgoldberg.com',
      url='zachgoldberg.com/',
      packages=['tasksitter'],
      package_dir={'tasksitter': 'src/'},
      scripts=['run_tasksitter']
     )
