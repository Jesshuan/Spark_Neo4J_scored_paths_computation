import sys
import getopt

from datetime import datetime

def usage__paths_computer():

    print("You have to specify : \n \
          -m (or --mode=) 'weighted' or 'equiprobable'(by default) \n \
          -e (or --experiment_name=) followed by the name of the experiment ('experiment_[timestamp] by default)" )
    

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

    print(f"Mode : {mode}")
    print(f"Experiment_name : {experiment_name}")

    return mode, experiment_name