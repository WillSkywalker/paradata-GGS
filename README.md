# paradata-GGS
A package for analyzing paradata from the Generations &amp; Gender Surveys.

## Installation

Install the package through `pip`:

```sh
pip install paradata
```

## Usage
To use the package, you can run the main script:

```sh
paradata [-h] [-s SEP] [-m {simple,switches}] [-t | --tablet | --no-tablet] input_filename output_filename
```

The `input_filename` should be an existing file containing Blaise paradata, and the `output_filename` must not exist, or it could be overwritten. Use `paradata -h` to explore all options.

You can also import the package in your own scripts:

```py
import pandas as pd
from paradata.parser import ParadataSessions

# Example usage
data = pandas.read_csv('path/to/your/data.csv')
sessions = ParadataSessions(data)
```
