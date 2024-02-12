import streamlit as st
import pandas as pd
import json
# my_comannds = []
# Initialize session state for command log and DataFrame if not already present
if 'command_log' not in st.session_state:
    st.session_state.command_log = []
    st.session_state.command_log_g = []

if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()

sheet_names = []  # Initialize an empty list for sheet names

def append_command(command):
    """Append a command to the command log in the session state."""
    if command["command"]!="select_sheet":
        if command["command"] not in ['rm','RM']:
            st.session_state.command_log_g.append(command)
        else:
            st.session_state.command_log_g = st.session_state.command_log_g[:-1]
    else:
        st.session_state.command_log.append(command)    
    # print(my_comannds)
    print(st.session_state.command_log)
    print(st.session_state.command_log_g)
    
    
def load_file(file):
    """Load and return a DataFrame from a CSV or Excel file, including sheet names for Excel."""
    global sheet_names
    if file.name.endswith('.csv'):
        df = pd.read_csv(file)
        sheet_names = []
    elif file.name.endswith('.xlsx'):
        xl = pd.ExcelFile(file)
        df = pd.read_excel(file, sheet_name=xl.sheet_names[0])  # Load the first sheet by default
        sheet_names = xl.sheet_names
    else:
        df = pd.DataFrame()
        sheet_names = []
    
    st.session_state.df = df  # Update the session state with the new DataFrame
    st.session_state.command_log = []  # Clear the command log for the new file
    return df

def load_excel_sheet(file, sheet_name):
    """Load a specific sheet from an Excel file into a DataFrame and log the action."""
    df = pd.read_excel(file, sheet_name=sheet_name)
    st.session_state.df = df
    append_command({"command": "select_sheet", "sheet_name": sheet_name})
    return df

def execute_command(command,key='de_1',log=True):
    """Execute a Python command on the DataFrame and log the action."""
    try:
        #prevent infinit loop on from file case 
        if log: append_command({"command": command})
        local_dict = {'df': st.session_state.df}
        for my_command in st.session_state.command_log_g:
            exec(my_command["command"], globals(), local_dict)
        st.session_state.df = local_dict['df']
        st.data_editor(st.session_state.df,key=key)
        # st.session_state.command_log.append({"command": command})
        # print(st.session_state.command_log)
        # print('done',command)
    except Exception as e:
        st.error(f"Error executing command: {e}")

def save_files(file_name):
    """Save the DataFrame and command log to JSON files."""

    if not st.session_state.df.empty:
        st.session_state.df.to_json(f"{file_name}.json", orient='split')
        with open(f"{file_name}_commands.json", 'w') as f:
            json.dump(st.session_state.command_log, f)
        with open(f"{file_name}_commands_edit.json", 'w') as f:
            json.dump(st.session_state.command_log_g, f)
        
        st.success(f"Files saved: {file_name}.json and {file_name}_commands.json")
    else:
        st.error("No DataFrame to save.")

# UI Elements
st.title("DataFrame Manipulator")


if __name__=='__main__':
    my_comannds=[]
    uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=['csv', 'xlsx'],key='upload_csv_ex')
    if uploaded_file is not None:
        df = load_file(uploaded_file) 
        st.session_state.df = df # Load the file and initialize the DataFrame
        # st.write("Loaded DataFrame:", st.session_state.df)  # Show the loaded DataFrame
        st.data_editor(st.session_state.df,key='de_2')

    if sheet_names:  # Show sheet selection only if there are sheets to select
        selected_sheet_name = st.selectbox("Select a sheet", sheet_names, index=0)
        if selected_sheet_name :
            df = load_excel_sheet(uploaded_file, selected_sheet_name)
            st.session_state.df = df
            # st.write(f"DataFrame from selected sheet '{selected_sheet_name}':", st.session_state.df)
            st.data_editor(st.session_state.df)

    command = st.text_input("Enter a command to execute on the DataFrame",key='ti_cmd')
    if st.button("Execute Command",key='excc'):
        execute_command(command)
        # st.write("Updated DataFrame:", st.session_state.df)

    if st.button("Save",key='sav'):
        file_name, _ = uploaded_file.name.rsplit('.', 1)
        save_files(file_name)

    command_file = st.file_uploader("Upload a command JSON file", type=['json'], key='command_json_uploader')
    if command_file is not None:
        command_data = json.load(command_file)
        st.session_state.command_log_g = command_data  # Replace the command log
        st.write("Loaded commands:", command_data)

    if st.button("Execute Commands from File",key='fff'):
        if st.session_state.command_log_g:
            execute_command("run it all",key='from_f',log=False)
            st.write("DataFrame after executing commands:")
            st.dataframe(st.session_state.df)
