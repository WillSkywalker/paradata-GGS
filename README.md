# paradata-GGS
A package for analyzing paradata from the Generations &amp; Gender Surveys.

## Installation

Install the package through ```pip```:

```sh
pip install paradata
```

## Usage
To use the package, you can run the main script:

```sh
paradata [-h] [-s SEP] [-m {simple,switches}] [-t | --tablet | --no-tablet] input_filename output_filename
```

You can also import the package in your own scripts:

```py
from paradata import parser

# Example usage
data = parser.parse('path/to/your/data.csv')
```
