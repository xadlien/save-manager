from setuptools import setup

setup(name='save-manager',
      version='0.1',
      description='Program to sync directories between devices/directories.',
      url='https://github.com/xadlien/save-manager',
      author='Daniel Martin',
      author_email='djm24862@gmail.com',
      packages=['savemanager'],
      entry_points = {
          'console_scripts': ['save-manager=savemanager.save_manager:main']
      },
      zip_safe=False)
