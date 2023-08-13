import argparse
from string import Template

# EG:
#
# ❯ python make_just.py --name blue-whale


class DblDollars(Template):
    delimiter = '$$'  # Use $$ as the delimiter

# Define the substitutions
substitutions = {
    "NAME": "niftynetwork",
    "NETWORKS": '`echo $HOME` + "/networks"',
    "GO_ALGORAND": "/Users/zeph/github/algorand/go-algorand",
    "NODE_TEMPLATE": "OneNodeFuture.json",
    "PRIVATE_DATA_NODE": "Primary",
}

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--just-out-file", default="Justfile.tmp")
parser.add_argument("--name")
parser.add_argument("--networks")
parser.add_argument("--go-algorand")
parser.add_argument("--node-template")
parser.add_argument("--private-data-node")


args = parser.parse_args()
templ_args = vars(args)
file_out = templ_args.pop("just_out_file")

# Update the substitutions with the arguments
substitutions.update({
    key: templ_args[k] for key in substitutions if templ_args[(k := key.lower())] is not None
})

# Open the template and substitute the placeholders with actual values
with open('Justfile.tmpl', 'r') as f:
    template = DblDollars(f.read())
    result = template.substitute(substitutions)

# Write the result to the output file
with open(file_out, 'w') as f:
    f.write(result)

