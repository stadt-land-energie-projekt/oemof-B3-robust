import os
import csv
import json
import random
import shutil

def clear_directory(directory):
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Error clearing directory: {str(e)}")
    else:
        os.makedirs(directory, exist_ok=True)

def count_scenarios(perturbation_data, no_of_perturbations):
    total = 0
    for perturbation in perturbation_data:
        if perturbation["PerturbationMethod"] in ["multiplication", "addition"]:
            it = len(perturbation["PerturbationParameter"])
            total += it
        else:
            it = len(perturbation["PerturbationParameter"]) * no_of_perturbations
            total += it
    return total

#Change for different modification methods
def apply_perturbation(value, method, parameter):
    if method == "multiplication":
        return value * parameter
    elif method == "addition":
        return value + parameter
    elif method == "distribution_uni":
        return value * random.uniform(parameter["a"], parameter["b"])
    elif method == "distribution_stdNorm":
        #for normal distribution: mean = original value * m, sigma = original value * s
        #we replace, not multiply
        return random.gauss(value * parameter["m"], value * parameter["s"])
    else:
        return value  #No perturbation

def modify_scenario_csv_struct(variable_type, variable_name, param, perturbation_method):

    #Define the input and output file names
    input_file = "./raw.backup/scalars/costs_efficiencies.csv"
    output_file = "./raw/scalars/modified_costs_efficiencies.csv"

    #Read the input file and store the data in a list of dictionaries
    data = []
    header = []
    with open(input_file, "r") as file:
        reader = csv.reader(file, delimiter=';')
        header = next(reader)
        for row in reader:
            data.append(row)

    #Find the indices of required columns in the header
    var_value_index = header.index("var_value")
    var_name_index = header.index("var_name")
    tech_index = header.index("tech")
    #perturbation only for the specific scenario
    #fixed for now - maybe add the key to json file as well
    key_index = header.index("scenario_key")
    var_value = 0
    temp_var_value = 0
    random_param = 0

    #Modify the var_value for the matching rows
    #For gt and bpchp careful bc there is 2 of them 
    #temp solution for marginal cost calc
    #same should be for storage but storage = 0 so meaningless
    if variable_name in ["bpchp", "gt"]:
        for row in data:
            if row[var_name_index] == variable_type and row[tech_index] == variable_name and row[key_index] == "2050-base":
                var_value = float(row[var_value_index])
                temp_var_value = apply_perturbation(var_value, perturbation_method, param)
                break
        
        for row in data:
            if row[var_name_index] == variable_type and row[tech_index] == variable_name and row[key_index] == "2050-base":
                if perturbation_method in ["multiplication", "addition"]:
                    var_value = temp_var_value
                else:
                    random_param = temp_var_value - var_value
                    var_value = temp_var_value
                var_value = round(var_value, 2)  #Round to 2 decimal places
                row[var_value_index] = str(var_value)
    else:
        for row in data:
            if row[var_name_index] == variable_type and row[tech_index] == variable_name and row[key_index] == "2050-base":
                var_value = float(row[var_value_index])
                temp_var_value = apply_perturbation(var_value, perturbation_method, param)
                if perturbation_method in ["multiplication", "addition"]:
                    var_value = temp_var_value
                else:
                    random_param = temp_var_value - var_value
                    var_value = temp_var_value
                var_value = round(var_value, 2)  #Round to 2 decimal places
                row[var_value_index] = str(var_value)
    
    #Write the modified data to the output file
    with open(output_file, "w", newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(header)
        writer.writerows(data)
    
    #Deleting old raw and replacing with new one
    os.system(f"rm ./raw/scalars/costs_efficiencies.csv")
    os.system(f"mv ./raw/scalars/modified_costs_efficiencies.csv ./raw/scalars/costs_efficiencies.csv")

    print("Modifications complete. The updated data has been written to ./raw/scalars/costs_efficiencies.csv")

    return random_param

def run_modified(scenario_name, new_scenario_name, perturbation_data, variable_type, preferences, start_folder):

    #MAYBE ADD TO USER INPUT
    nb_cores = 8
    
    #Generating the new scenario
    os.system(f"cp ./scenarios/{scenario_name}.yml ./scenarios/{new_scenario_name}.yml")
    #Backup the raw data
    os.system(f"cp -r ./raw/ ./raw.backup")
    #Clean any prior results
    os.system(f"snakemake -j1 clean")

    results_directory = f"./results/{new_scenario_name}"

    if variable_type == "marginal_cost":
        cost_type = "MarginalCosts"
    elif variable_type == "capacity_cost_overnight":
        cost_type = "CapacityCosts"
    elif variable_type == "storage_capacity_cost_overnight":
        cost_type = "StorageCosts"
            

    for perturbation in perturbation_data:
        
        if perturbation["PerturbationMethod"] in ["multiplication", "addition"]:
            i=1
            it = len(perturbation["PerturbationParameter"])
            
            for param in perturbation["PerturbationParameter"]:
                print("-----")
                print("RUNNING " + scenario_name + " SCENARIO WITH " + variable_type + " " + perturbation["PerturbationMethod"] + " PERTURBATION FOR " + perturbation["VariableName"] + ": " + str(i) + "/" + str(it))
                print("-----")
                
                random_param = modify_scenario_csv_struct(variable_type, perturbation["VariableName"], param, perturbation["PerturbationMethod"])
                
                os.system(f"snakemake -j{nb_cores} results/{new_scenario_name}/postprocessed")

                perturbation_info = {
                    "Preferences": {
                        "SettingName": preferences["SettingName"],
                        "SettingValue": preferences["SettingValue"],
                        "NumberOfPerturbations": 1,
                        "BaseScenName": preferences["BaseScenName"]
                    },
                    "CostPerturbations": {
                        cost_type: [
                            {
                                "VariableName": perturbation["VariableName"],
                                "PerturbationMethod": perturbation["PerturbationMethod"],
                                "PerturbationParameter": [param],
                                "VariableUnit": perturbation["VariableUnit"]
                            }
                        ]
                    }
                }

                #Copy the results to the numbered folder
                folder_name = os.path.join("robust_results", str(start_folder), new_scenario_name)
                shutil.copytree(results_directory, folder_name)
                #Write the perturbation info to a JSON file
                folder_name = os.path.join("robust_results", str(start_folder))
                json_file_path = os.path.join(folder_name, "scenario.json")
                with open(json_file_path, "w") as new_json_file:
                    json.dump(perturbation_info, new_json_file, indent=4)
                start_folder += 1

                os.system(f"snakemake -j1 clean")
                
                i += 1           
        else:
            i=1
            it = len(perturbation["PerturbationParameter"]) * preferences["NumberOfPerturbations"]
            for param in perturbation["PerturbationParameter"]:
                for n in range(preferences["NumberOfPerturbations"]):
                    print("-----")
                    print("RUNNING " + scenario_name + " SCENARIO WITH " + variable_type + " " + perturbation["PerturbationMethod"] + " PERTURBATION FOR " + perturbation["VariableName"] + ": " + str(i) + "/" + str(it))
                    print("-----")
                
                    random_param = modify_scenario_csv_struct(variable_type, perturbation["VariableName"], param, perturbation["PerturbationMethod"])
                
                    perturbation_info = {
                    "Preferences": {
                        "SettingName": preferences["SettingName"],
                        "SettingValue": preferences["SettingValue"],
                        "NumberOfPerturbations": 1,
                        "BaseScenName": preferences["BaseScenName"]
                    },
                    "CostPerturbations": {
                        cost_type: [
                            {
                                "VariableName": perturbation["VariableName"],
                                "OriginalPerturbationMethod": perturbation["PerturbationMethod"],
                                "OriginalPerturbationParameter": [param],
                                "PerturbationMethod": "addition",
                                "PerturbationParameter": [random_param],
                                "VariableUnit": perturbation["VariableUnit"]
                            }
                        ]
                    }
                }

                    os.system(f"snakemake -j{nb_cores} results/{new_scenario_name}/postprocessed")

                    #Copy the results to the numbered folder
                    folder_name = os.path.join("robust_results", str(start_folder), new_scenario_name)
                    shutil.copytree(results_directory, folder_name)
                    #Write the perturbation info to a JSON file
                    folder_name = os.path.join("robust_results", str(start_folder))
                    json_file_path = os.path.join(folder_name, "scenario.json")
                    with open(json_file_path, "w") as new_json_file:
                        json.dump(perturbation_info, new_json_file, indent=4)
                    start_folder += 1

                    i += 1
            
    #ToThink: maybe do it after each perturbation group
    #Bringing back original raw data
    os.system(f"rm -r ./raw")
    os.system(f"mv ./raw.backup ./raw")

    #Removing the modified scenario
    os.system(f"rm ./scenarios/{new_scenario_name}.yml")
    
if __name__ == "__main__":

    #Create or clear the 'robust_results' directory at the beginning
    clear_directory("./robust_results")
    
    with open('JSONFile_scenarios.json', 'r') as json_file:
        input_data = json.load(json_file)
    
    #Scenario name choice from JSON file
    scenario_name = input_data["Preferences"]["BaseScenName"]
    modification_suffix = "_mod"
    new_scenario_name = scenario_name + modification_suffix

    no_of_perturbations = input_data["Preferences"]["NumberOfPerturbations"]
    preferences = input_data["Preferences"]

    marginal_cost_perturbation_data = []
    capacity_cost_perturbation_data = []
    storage_cost_perturbation_data = []

    if "CostPerturbations" in input_data:
        if "MarginalCosts" in input_data["CostPerturbations"]:
            marginal_cost_perturbation_data += input_data["CostPerturbations"]["MarginalCosts"]
        if "CapacityCosts" in input_data["CostPerturbations"]:
            capacity_cost_perturbation_data += input_data["CostPerturbations"]["CapacityCosts"]
        if "StorageCosts" in input_data["CostPerturbations"]:
            storage_cost_perturbation_data += input_data["CostPerturbations"]["StorageCosts"]
    #NOT APPLICABLE YET
    '''
    if "VariablePerturbations" in input_data["Preferences"]:
        perturbation_data += input_data["Preferences"]["VariablePerturbations"]
    if "BoundPerturbations" in input_data["Preferences"]:
        perturbation_data += input_data["Preferences"]["BoundPerturbations"]
    '''
    
    #Calculate the total number of runs
    runs_marginal = count_scenarios(marginal_cost_perturbation_data, no_of_perturbations)
    runs_capacity = count_scenarios(capacity_cost_perturbation_data, no_of_perturbations)
    runs_storage = count_scenarios(storage_cost_perturbation_data, no_of_perturbations)

    run_modified(scenario_name, new_scenario_name, marginal_cost_perturbation_data, "marginal_cost", preferences, 1)
    run_modified(scenario_name, new_scenario_name, capacity_cost_perturbation_data, "capacity_cost_overnight", preferences, runs_marginal+1)
    run_modified(scenario_name, new_scenario_name, storage_cost_perturbation_data, "storage_capacity_cost_overnight", preferences, runs_marginal+runs_capacity+1)
