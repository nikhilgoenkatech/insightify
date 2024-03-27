from setuptools import setup, find_packages

setup(name="insightify",
      version="0.0.42",
      description="Insightify python EF2 extension",
      author="Dynatrace",
      packages=find_packages(),
      python_requires=">=3.10",
      include_package_data=True,
      install_requires=["dt-extensions-sdk", "requests<3", "charset_normalizer<3"],
      extras_require={"dev": ["dt-extensions-sdk[cli]"]},
      )
