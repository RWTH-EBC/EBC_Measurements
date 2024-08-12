import setuptools

setuptools.setup(
    name='EBC_Measurements',
    version='0.1',
    packages=['ebcmeasurements', 'ebcmeasurements.Base', 'ebcmeasurements.Beckhoff',
              'ebcmeasurements.Sensor_Electronic'],
    url='https://github.com/RWTH-EBC/EBC_Measurements',
    license='MIT',
    author='RWTH Aachen University, E.ON Energy Research Center, '
           'Institute for Energy Efficient Buildings and Indoor Climate',
    author_email='ebc-abos@eonerc.rwth-aachen.de',
    description='A tool for different measurement devices'
)
