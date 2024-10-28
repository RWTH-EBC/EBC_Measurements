from setuptools import setup, find_packages

setup(
    name='EBC-Measurements',
    version='1.1.0',
    author='RWTH Aachen University, E.ON Energy Research Center, '
           'Institute for Energy Efficient Buildings and Indoor Climate',
    author_email='ebc-abos@eonerc.rwth-aachen.de',
    description='All-in-One Toolbox for Measurement Data Acquisition and Data Logging',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/RWTH-EBC/EBC_Measurements',
    packages=find_packages(),
    license='MIT',
    install_requires=[
        'pyserial~=3.5',
        'pyads~=3.4.2',
        'setuptools~=72.1.0',
        'paho-mqtt~=2.1.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)
