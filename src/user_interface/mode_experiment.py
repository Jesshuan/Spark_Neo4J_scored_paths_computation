import sys
import os
import getopt

from datetime import datetime

from variables.memory_path import HISTORICAL_BATCHES_FOLDER


# ---- User Interface functions ---- #

# Management of args provided by the user for the 2 scripts
# with the getopt library

# -- Sub-functions --- #

# help instructions (depend on the script)

def usage__paths_computer():

    print("You have to specify : \n \
          -m (or --mode=) 'weighted' or 'equiprobable'(by default) \n \
          -e (or --experiment_name=) followed by the name of the experiment ('experiment_[timestamp] by default)" )
    
    return
    
def usage__spark_agregator():

    print("You have to specify : \n \
          -m (or --mode=) 'weighted' or 'equiprobable'(by default) \n \
          -e (or --experiment_name=) followed by the name of the existing experiment (necessary) \n \
          -f (or --fast_recomputation_inspired_by=) followed by the name of the experiment provided for the fast recomputation (no default value) \n" )
    
    return

def usage__export_map():

    print("You have to specify : \n \
          -e (or --experiment_name=) followed by the name of the existing experiment (necessary)" )
    
    return


# -- Main functions --- #

def mode_experiment_paths_computer(args_list):

    try:                                
        opts, args = getopt.getopt(args_list, "hm:e:", ["help", "mode=", "experiment_name="])
    except getopt.GetoptError:           
        usage__paths_computer()                          
        sys.exit(2)

    mode = "equiprobable" # by default

    experiment_name = "experiment_" + str(datetime.now()).split(".")[0].replace(" ","_")


    for opt, arg in opts:

        if opt in ("-h", "--help"):
            print("--- help for shell command ---")     
            usage__paths_computer()                      
            sys.exit(0) 

        elif opt in ("-m", "--mode"):

            if arg not in ["weighted", "equiprobable"]:
                print("argument not recognized...")
                usage__paths_computer()                     
                sys.exit(2)
            else:        
                mode = arg
                                          
        elif opt in ("-e", "--experiment_name"):
            experiment_name = arg
        else:
            print("argument not recognized...")
            usage__paths_computer()                     
            sys.exit(2)

    print("")
    print("------------###############---------------")
    print("----------------######--------------------")
    print("")
    print(f"Mode : {mode}")
    print(f"Experiment_name : {experiment_name}")
    print("")
    print("----------------######--------------------")
    print("------------###############---------------")
    print("")

    return mode, experiment_name




def mode_experiment_spark_agregator(args_list):

    try:                                
        opts, args = getopt.getopt(args_list, "hm:e:f:", ["help", "mode=", "experiment_name=", "fast_recomputation_inspired_by="])
    except getopt.GetoptError:           
        usage__spark_agregator()                          
        sys.exit(2)

    mode = "equiprobable" # by default

    experiment_name = "experiment_" + str(datetime.now()).split(".")[0].replace(" ","_")

    fast_recomp = False # No Fast recomputation mode by default

    exp_fr = None # no provided by default

    print(opts)

    for opt, arg in opts:

        if opt in ("-h", "--help"):
            print("--- help for shell command ---")     
            usage__spark_agregator()                      
            sys.exit(0) 

        elif opt in ("-m", "--mode"):

            if arg not in ["weighted", "equiprobable"]:
                print("argument not recognized...")
                usage__spark_agregator()                     
                sys.exit(2)
            else:        
                mode = arg
                                          
        elif opt in ("-e", "--experiment_name"):
            experiment_name = arg

        elif opt in ("-f", "--fast_recomputation_inspired_by"):
            fast_recomp = True
            exp_fr = arg

        else:
            print("argument not recognized...")
            usage__spark_agregator()                     
            sys.exit(2)

    # In case of Fast-Recomputation mode, test if the experiment exists with historical data :
    
    if fast_recomp:
        path_exp_fr = HISTORICAL_BATCHES_FOLDER + exp_fr + ".pkl"

        if not os.path.exists(path_exp_fr):
            print("For Fast Recomputation mode, the experiment doesn't exist... or the folder is empty ?")
            usage__spark_agregator()                     
            sys.exit(2)

    print("")
    print("------------***************---------------")
    print("----------------******--------------------")
    print("")
    print(f"Mode : {mode}")
    print(f"Experiment_name : {experiment_name}")
    print(f"Fast_recomputation mode : {fast_recomp}")
    if fast_recomp:
        print(f"-- inspired_by : {exp_fr}")
    print("")
    print("----------------******--------------------")
    print("------------***************---------------")
    print("")
    

    

    return mode, experiment_name, fast_recomp, exp_fr




def experiment_export_map(args_list):

    try:   

        if len(args_list) < 2:
            usage__export_map()
            print("Please, give an argument for experiment_name...")
            sys.exit(2)

        opts, args = getopt.getopt(args_list, "he:", ["help", "experiment_name="])
    except getopt.GetoptError:           
        usage__export_map()                          
        sys.exit(2)

    for opt, arg in opts:

        if opt in ("-h", "--help"):
            print("--- help for shell command ---")     
            usage__export_map()                      
            sys.exit(0) 
                                          
        elif opt in ("-e", "--experiment_name"):
            experiment_name = arg
        else:
            print("argument not recognized...")
            usage__export_map()                    
            sys.exit(2)

    print("")
    print("------------###############---------------")
    print("Export final map result -> html file")
    print(f"Experiment_name : {experiment_name}")
    print("------------###############---------------")
    print("")

    return experiment_name