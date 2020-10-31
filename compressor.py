import pandas as pd
import numpy as np
import pickle

def DataCompress(df, category_percent=0.33, verbose=0, pickle_name=None, zip_name=None):

    """
    Function that takes in a dataframe and recasts types to lower its memory footprint

    This function will read in a dataframe and parse column by column,
    checking to see if it is using unnecessary memory based on its content
    (i.e. int64 when int16 is sufficient) and recasting these types.
    Will also recast object variables as category variables based on
    what percentage of values are unique. By default, if the number of unique values
    divided by the total number of values is less than 0.33 then it will be
    recast as a category variable, but this threshold can be set to anything.
    The verbose keyword allows you to see what percentage your memory footprint
    decreased by as a whole (1) or by each indivdual column (2). There is
    also the option to package this compressed dataframe as a pickle or zip file.

    Parameters
    ----------

    df : Dataframe
        The dataframe that will be compressed in terms of memory footprint and
        will optionally packaged as a pickle or zip file

    category_percent : float
        The percentage of unique values to be used as the threshold to cast
        a variable as a category variable

    verbose : int
        Used to determine whether no information about memory reduction will
        be printed (0), information about the total memory reduction will be
        printed (1), or information about the memory reduction of each individual
        column will be printed (2)

    pickle_name : string
        The name that will be used for the pickle file being created (do not include
        any value for this if you do not want a pickle file created)

    zip_name : string
        The name that will be used for the zip file being created (do not include
        any value for this if you do not want a zip file created)

    Returns
    -------

    No explicit returns but will print values based on the value of the
    verbose argument and will alter(compress) the input dataframe

    Raises
    ------
    ValueError
        If the first argument is either not provided or not a dataframe

    ValueError
        If the category_percent argument is not a number in between 0 and
        1 inclusive

    ValueError
        If the verbose argument is not a number equal to 0, 1, or 2

    ValueError
        If the pickle_name argument is not a string

    ValueError
        If the zip_name argument is not a string

    Notes
    -----

    Please note that once this function is run then the dataframes
    memory footprint has already been compressed, running this function
    multiple times on the same dataframe will not lead to any further
    changes after the first run, and having the verbosity set to anything
    other than 0 will show that all changes in memory are 0% on any
    function call after the first with the same dataframe


    """
    if type(df) is not pd.core.frame.DataFrame:
        raise ValueError("Must provide Dataframe")
    initial_memory = df.memory_usage()
    initial_memory_sum = sum(df.memory_usage())
    for i in df.columns:
        if category_percent < 0 or category_percent > 1:
            raise ValueError("Value must be in between 0 and 1")
        if (isinstance(df[i][0], str)) and ((len(df[i].unique()) / len(df[i])) < category_percent):
            df[i] = df[i].astype('category', errors='ignore')
        elif (isinstance(df[i][0], str)):
            df[i] = df[i].astype(str, errors='ignore')
        elif np.array_equal(df[i], df[i].astype(int, errors='ignore')):
            if abs(df[i].max()) < 130:
                df[i] = df[i].astype('int8', errors='ignore')
            elif abs(df[i].max()) < 33000:
                df[i] = df[i].astype('int16', errors='ignore')
            elif abs(df[i].max()) < 2147483700:
                df[i] = df[i].astype('int32', errors='ignore')
            else:
                df[i] = df[i].astype('int64', errors='ignore')
        else:
            df[i] = df[i].astype('float64', errors='ignore')
    if verbose not in [0,1,2]:
        raise ValueError("Value must equal 1, 2, or 3")
    if verbose == 1:
        print(((initial_memory_sum - sum(df.memory_usage())) / initial_memory_sum) * 100)
    elif verbose == 2:
        for i in range(len(initial_memory) - 1):
            print(f"{df.columns[i]} : {((initial_memory[i] - df.memory_usage()[i]) / initial_memory[i]) * 100}")
    if not isinstance(pickle_name, str) and pickle_name is not None:
        raise ValueError("Value must be a string")
    if pickle_name != None:
        pickling_on = open(pickle_name, "wb")
        pickle.dump(df, pickling_on)
        pickling_on.close()
    if not isinstance(zip_name, str) and zip_name is not None:
        raise ValueError("Value must be a string")
    if zip_name != None:
        df.to_csv(zip_name, compression = 'zip')
