from setuptools import setup, find_packages

def run_setup():
    setup(

       name='astronomer',
        version='0.1',
        packages=find_packages(),
    )

if __name__ == "__main__":
    run_setup()