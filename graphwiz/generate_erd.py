import graphviz
import re 

# TypeScript code
with open('event-definition-type.ts') as def_f:
    typescript_code=def_f.read()
# ... [Previous script code for ts_interfaces_to_graphviz function] ...

# Function to convert TypeScript interfaces to Graphviz DOT format
def ts_interfaces_to_graphviz(ts_code):
    graphviz_output = "digraph G {\n    node [shape=plaintext];\n"
    for interface_block in re.finditer(r'export interface (\w+)\s*{([^}]*)}', ts_code):
        interface_name, properties_block = interface_block.groups()
        graphviz_str = f'    {interface_name} [label=<\n        <table border="0" cellborder="1" cellspacing="0" cellpadding="4">\n'
        graphviz_str += f'            <tr><td bgcolor="lightblue">{interface_name}</td></tr>\n'
        for prop, optional, type in re.findall(r'(\w+)(\??)\s*:\s*([^;]+);', properties_block):
            # Remove '?' from port name and use property name until ':'
            clean_prop = prop.replace("?", "")
            graphviz_str += f'            <tr><td align="left" port="{clean_prop}">{prop}{"?" if optional else ""}: {type}</td></tr>\n'                 
        graphviz_str += '        </table>\n    >];\n'
        graphviz_output += graphviz_str
                                                                                                # Add additional edges here if necessary
    graphviz_str = 'ReportedEventDefinition:id->ReportedEventDefValue:id;\n'
    graphviz_output += graphviz_str 

    graphviz_output += "}\n"
    return graphviz_output

#Generate Graphviz output
dot_output = ts_interfaces_to_graphviz(typescript_code)

# Print the DOT output for debugging
print(dot_output)

# Create a Graphviz object
dot = graphviz.Source(dot_output, format='pdf')

# Save the PDF file (without '.pdf' in the filename)
pdf_filename = '/home/ubuntu/workspaces/mywr/http/output_graph'
dot.render(pdf_filename, view=False)
