import sys


from user_interface.mode_experiment import mode_experiment_spark_agregator



if __name__ == "__main__":

    mode, experiment_name, fast_recomp, exp_fr = mode_experiment_spark_agregator(sys.argv[1:])