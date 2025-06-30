from setuptools import setup

setup(
    name='OMQ',
    version='1.0.2',
    description='Message broker design for De ontzorgers (https://www.deontzorgers.nl)',
    url='https://github.com/shuds13/pyexample',
    author='De Ontzorgers',
    author_email='service@deontzorgers.nl',
    license='BSD 2-clause',
    packages=['omq'],
    install_requires=[
        'SQLAlchemy==2.0.41',
        'PyMySQL==1.1.1',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: Dutch',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
    ],
)
