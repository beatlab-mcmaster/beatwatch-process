# beatwatch-process

Python package for processing data collected with Bangle.js 2 and the BEATwatch application

## Installation

### Install the beatwatch-process package

#### uv

```{sh}
uv add git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git
```

Or, install a branch, e.g.:

```sh
uv add git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git@branch-name
```

Upgrade to a new version with:

```sh
uv add --upgrade git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git
```

Uninstall with:

```sh
uv remove beatwatch-process
```

#### pip

```sh
pip install git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git
```

Or, install a branch, e.g.:

```sh
pip install git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git@branch-name
```

Upgrade to a new version with:

```sh
pip install --upgrade git+ssh://git@github.com/beatlab-mcmaster/beatwatch-process.git
```

Uninstall with:

```sh
pip uninstall beatwatch-process
```

## Usage

## Troubleshooting

I keep getting an error: "`ModuleNotFoundError: No module named 'beatwatch_process'`"
when attempting to run scripts. E.g.:

- `uv run beatwatch-process`
- `uv run python tests/test_output.py`

To reset:

```sh
uv run --reinstall
```
