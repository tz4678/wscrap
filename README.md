# WScrap

Command line web scraping tool.

Usage:

```zsh
$ pip install wscrap
# The output format is JSONL. Use jq to parse it.
$ wscrap -i domain_list.txt -o resutls.json -vv 2> log.txt

# or without install
$ pipx run wscrap -h
```
