from setuptools import setup
setup(
    name='rtestnet',
    install_requires=[
        'aioredis',
        'quart',
        'hypercorn',
        'pyhocon',
        'schema',
        'deepmerge'
    ],
)
