#!/bin/bash

mkdir -p empty
# don't bind this to a public interface!
python3 -m http.server --bind "127.0.0.1" -d empty 8000
rmdir empty

