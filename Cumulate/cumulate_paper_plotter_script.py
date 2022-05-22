import os

import matplotlib
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.legend import Legend
from matplotlib.offsetbox import AnchoredText
from matplotlib.pyplot import figure
from matplotlib.ticker import StrMethodFormatter


def parse_and_plot_files(experiment_key, x_label, x_ticks, y_label, y_ticks):
    path = os.getcwd() + '\\Experiments\\'
    files = []
    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path, i)) and i.startswith(experiment_key) and i.endswith('plot.txt'):
            files.append(os.path.join(path, i))

    x_axis = []  # Equal for all algorithms
    y_axis_dict = {}  # This has multiple list for each algorithm in the same plot
    for filepath in files:
        filename = os.path.basename(filepath)
        str1 = filename.replace(experiment_key + '_', '')
        key = str1.replace('_plot.txt', '')
        y_axis_dict[key] = []  # Define keys based on each algorithm file
        with open(filepath) as file:
            lines = file.readlines()
        for line in lines:
            string_split = line.rstrip().split(",")
            if string_split[0] == 'x' and float(string_split[1]) not in x_axis:
                x_axis.append(float(string_split[1]))  # Value of x-coordinate
            elif string_split[0] == 'y':
                y_axis_dict[key].append(float(string_split[1]))  # Value of y-coordinate

    x_axis = sorted(list(set(x_axis)))

    cumulate_vanilla_time = y_axis_dict['cumulate_vanilla']
    cumulate_vertical_time = y_axis_dict['cumulate_vertical']
    cumulate_vertical_with_parallel_count_time = y_axis_dict['cumulate_vertical_with_parallel_count']

    # Check if avg time in seconds surpasses a threshhold. If so, express time in minutes
    avg_time_in_seconds = (np.average(cumulate_vanilla_time) + np.average(cumulate_vertical_time) + np.average(
        cumulate_vertical_with_parallel_count_time)) // 3
    # if avg_time_in_seconds > 60:
    #     cumulate_vanilla_time = list(map(lambda x: x // 60, cumulate_vanilla_time))
    #     cumulate_vertical_time = list(map(lambda x: x // 60, cumulate_vertical_time))
    #     cumulate_vertical_with_parallel_count_time = list(
    #         map(lambda x: x // 60, cumulate_vertical_with_parallel_count_time))

    # For Transaction experiment re-scaling
    if experiment_key == 'transaction':
        cumulate_vanilla_time = scale_y_axis_by_unit(x_axis, cumulate_vanilla_time, 1)
        cumulate_vertical_time = scale_y_axis_by_unit(x_axis, cumulate_vertical_time, 1)
        cumulate_vertical_with_parallel_count_time = scale_y_axis_by_unit(x_axis, cumulate_vertical_with_parallel_count_time, 1)

    plt.plot(x_axis, cumulate_vanilla_time, linestyle='-', marker='o', color='r', label='cumulate')
    plt.plot(x_axis, cumulate_vertical_time, linestyle='--', marker='s',color='g', label='cumulate_vertical')
    plt.plot(x_axis, cumulate_vertical_with_parallel_count_time, linestyle='--', marker='d', color='b',
             label='cumulate_vertical_with_parallel_count')

    if experiment_key == 'support':
        plt.xlim(max(x_axis),min(x_axis))

    plt.xticks(x_ticks)
    plt.yticks(y_ticks)

    # Naming the x-axis, y-axis and the whole graph
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    # For Transaction experiment
    if experiment_key == 'transaction':
        plt.xlabel('TRANSACTIONS (millions)')
        plt.ylabel("Time / #TRANSACTIONS (normalized)")

    # if avg_time_in_seconds > 60:
    #     plt.ylabel("Time (Minutes)")
    # else:
    #     plt.ylabel("Time (Seconds)")

    plt.title(experiment_key.upper())

    # Adding legend, which helps us recognize the curve according to it's color
    plt.legend()

    plt.savefig('Plots/' + experiment_key.upper() + '.png', format="png")
    # plt.show()
    plt.close()


def scale_y_axis_by_unit(x_axis, y_axis, x_axis_target_unit):
    """
    Given two list of x and y coordinates and one x-coordinate target that will serve as a unit, rescale y-axis
    using x_axis_target_unit as unit ratio for all points
    :param x_axis:
    :param y_axis:
    :param x_axis_target_unit:
    :return:
    """
    points = list(zip(x_axis, y_axis))
    unit_ratio = next(x[1] for x in points if x[0] == x_axis_target_unit)
    y_axis_rescaled = []
    for p in points:
        z = p[1] / p[0]
        r = z / unit_ratio
        y_axis_rescaled.append(r)
    return y_axis_rescaled

def plot_parallel_count_efectiveness_experiment():
    """
    Experiment time log with Syntetic Dataset: Root - R250.data
    Sequential with min_sup: 0.0005
    Took 0.04490804672241211 seconds in k =1 with this amount of candidates: 55844
    Took 200.33265686035156 seconds in k =2 with this amount of candidates: 8522256
    Took 0.6751060485839844 seconds in k =3 with this amount of candidates: 828
    Took 0.0039920806884765625 seconds in k =4 with this amount of candidates: 8
    Took 0.0 seconds in k =5 with this amount of candidates: 1
    Took 0.0 seconds in k =6 with this amount of candidates: 0

    Parallel with min_sup: 0.0005
    Took 2.7735891342163086 seconds in k =1 with this amount of candidates: 55844
    Took 93.6016058921814 seconds in k =2 with this amount of candidates: 8522256
    Took 2.215111017227173 seconds in k =3 with this amount of candidates: 828
    Took 0.15401291847229004 seconds in k =4 with this amount of candidates: 8
    Took 0.031039953231811523 seconds in k =5 with this amount of candidates: 1
    Took 0.0 seconds in k =6 with this amount of candidates: 0
    :return:
    """
    fig, ax = plt.subplots()
    plt.xlabel('K')
    plt.ylabel('Time (seconds)')
    plt.xticks([1, 2, 3, 4, 5])
    plt.yticks([0, 10, 50, 100 , 200])

    plt.plot([1, 2, 3, 4, 5], [0.049, 200.332, 0.067, 0.004, 0.0], linestyle='-', marker='o', color='r', label='apriori')
    plt.plot([1, 2, 3, 4, 5], [2.773, 93.60, 2.21, 0.15, 0.031], linestyle='--', marker='s', color='g', label='apriori with parallelization')
    plt.title('Parallel Count')
    plt.legend()
    anchored_text = AnchoredText(
        'k=1: 55844 candidates \nk=2: 8522256 candidates\nk=3: 828 candidates\nk=4: 8 candidates\nk=5: 1 candidates',
                                 frameon=True, borderpad=0, pad=0.1,
                                 loc='upper right', bbox_to_anchor=[1.40, 1.],
                                 bbox_transform=plt.gca().transAxes,
                                 prop={'color': 'k'})
    ax.add_artist(anchored_text)

    plt.savefig('Plots/Parallel_Efectiveness.png', format="png", bbox_inches='tight')
    plt.close()

def plot_parallel_rule_gen_efectiveness_experiment():
    """
    Experiment time log with Synthetic Dataset: 10k transactions, 250 items

    Parallel with min_sup: 0.001
    Parallel rule_gen took 0.9892690181732178 seconds to check 11513 potential rules
    Sequential rule_gen took 0.005987882614135742 seconds to check 11513 potential rules
    Generated 1624 rules

    Parallel with min_sup: 0.0008
    Parallel rule_gen took 0.8298518657684326 seconds to check 15234 potential rules
    Sequential rule_gen took 0.001995086669921875 seconds to check 15234 potential rules
    Generated 2981 rules

    Parallel with min_sup: 0.0005
    Parallel rule_gen took 1.0595660209655762 seconds to check 64316 potential rules
    Sequential rule_gen took 0.02196502685546875 seconds to check 64316 potential rules
    Generated 31116 rules

    Parallel with min_sup: 0.0003
    Parallel rule_gen took 1.1898531913757324 seconds to check 122658 potential rules
    Sequential rule_gen took 0.04088997840881348 seconds to check 122658 potential rules
    Generated 68487 rules

    Parallel with min_sup: 0.0001
    Parallel rule_gen took 26.443121194839478 seconds to check 6900406 potential rules
    Sequential rule_gen took 22.4965500831604 seconds to check 6900406 potential rules
    Generated 6419499 rules

    :return:
    """
    fig, ax = plt.subplots()
    plt.xlabel('Minimum Support (%)')
    plt.ylabel('Time (seconds)')


    x_axis = list(reversed([0.1, 0.08, 0.05, 0.03, 0.01])) # Expressed in %
    y_axis_parallel_rule_gen = list(reversed([0.989, 0.829, 1.059, 1.189, 26.44]))
    y_axis_sequential_rule_gen = list(reversed([0.005, 0.001, 0.021, 0.040, 22.496]))

    plt.plot(x_axis, y_axis_sequential_rule_gen, linestyle='-', marker='o', color='r',
             label='sequential rule generation')
    plt.plot(x_axis, y_axis_parallel_rule_gen, linestyle='--', marker='s', color='g',
             label='parallel rule generation')
    plt.title('Rule Generation')
    plt.xlim(max(x_axis),min(x_axis))
    plt.legend()
    anchored_text = AnchoredText(
        'min_sup: 0.1%: 11513 potential rules \nmin_sup 0.08%: 15234 potential rules\nmin_sup 0.05%: 64316 potential rules\nmin_sup 0.03%: 122658 potential rules\nmin_sup 0.01%: 6900406 potential rules',
        frameon=True, borderpad=0, pad=0.1,
        loc='upper right', bbox_to_anchor=[1.60, 1.],
        bbox_transform=plt.gca().transAxes,
        prop={'color': 'k'})
    ax.add_artist(anchored_text)

    plt.savefig('Plots/Parallel_Rule_Gen_Efectiveness.png', format="png", bbox_inches='tight')
    plt.close()

def plot_rule_gen_by_potential_rule_size():
    """
    Generating random list of size 100000
    Sequential rule_gen took 0.006981372833251953 seconds to check 99999 potential rules

    Parallel rule_gen took 0.5893323421478271 seconds to check 99999 potential rules
    ---------------------------------------------------------
    Generating random list of size 500000
    Sequential rule_gen took 0.03388190269470215 seconds to check 499999 potential rules

    Parallel rule_gen took 0.6871805191040039 seconds to check 499999 potential rules
    ---------------------------------------------------------
    Generating random list of size 1000000
    Sequential rule_gen took 0.07280540466308594 seconds to check 999999 potential rules

    Parallel rule_gen took 0.8906168937683105 seconds to check 999999 potential rules
    ---------------------------------------------------------
    Generating random list of size 2000000
    Sequential rule_gen took 0.13905072212219238 seconds to check 1999999 potential rules

    Parallel rule_gen took 1.1448447704315186 seconds to check 1999999 potential rules
    ---------------------------------------------------------
    Generating random list of size 5000000
    Sequential rule_gen took 0.3480958938598633 seconds to check 4999999 potential rules

    Parallel rule_gen took 2.3328635692596436 seconds to check 4999999 potential rules
    ---------------------------------------------------------
    Generating random list of size 10000000
    Sequential rule_gen took 0.6993837356567383 seconds to check 9999999 potential rules

    Parallel rule_gen took 4.72350811958313 seconds to check 9999999 potential rules
    ---------------------------------------------------------
    Generating random list of size 20000000
    Sequential rule_gen took 1.4111578464508057 seconds to check 19999999 potential rules

    Parallel rule_gen took 9.735824823379517 seconds to check 19999999 potential rules
    ---------------------------------------------------------
    Process finished with exit code 0

        :return:
    """
    fig, ax = plt.subplots()
    # ax.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), '{:.f}')))
    # ax.xaxis.set_major_formatter(StrMethodFormatter('{:>12,.2f}'))

    plt.xlabel('Potential Rules (000s)')
    plt.ylabel('Time (seconds)')

    x_axis = [100, 500, 1000, 2000, 5000, 10000, 20000]
    y_axis_parallel_rule_gen =  [0.5893, 0.6871, 0.89061, 1.1448, 2.3328,4.7235, 9.7358 ]
    y_axis_sequential_rule_gen =  [0.0069, 0.0338, 0.0728, 0.1390, 0.3480, 0.6993, 1.4111 ]
    y_axis_diff = list(np.divide(np.array(y_axis_parallel_rule_gen), np.array(y_axis_sequential_rule_gen)))
    plt.plot(x_axis, y_axis_sequential_rule_gen, linestyle='-', marker='o', color='r',
             label='sequential rule generation')
    plt.plot(x_axis, y_axis_parallel_rule_gen, linestyle='--', marker='s', color='g',
             label='parallel rule generation')
    # plt.plot(x_axis, y_axis_diff, linestyle='dashed', marker='d', color='b',
    #          label='diff')
    plt.title('Rule Generation')
    plt.legend()
    plt.savefig('Plots/Parallel_Rule_Gen_Efectiveness_Test.png', format="png", bbox_inches='tight')
    plt.close()

def plot_htgar_roots_experiment():
    """
    F:\TesisSyntheticDatasets\Root\R250T250k-timestamped.csv
    HTGAR took 295.95133996009827 seconds

    F:\TesisSyntheticDatasets\Root\R500T250k-timestamped.csv
    HTGAR took 158.4546139240265 seconds

    F:\TesisSyntheticDatasets\Root\R750T250k-timestamped.csv
    HTGAR took 136.51410102844238 seconds

    F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv
    HTGAR took 116.14133787155151 seconds
    ----------------------------------------------------------------
    F:\TesisSyntheticDatasets\Root\R250T250k.data
    Vertical Cumulate took 144.1433389186859 seconds

    F:\TesisSyntheticDatasets\Root\R500T250k.data
    Vertical Cumulate took 69.22877907752991 seconds

    F:\TesisSyntheticDatasets\Root\R750T250k.data
    Vertical Cumulate took 59.64191699028015 seconds

    F:\TesisSyntheticDatasets\Root\R1000T250k.data
    Vertical Cumulate took 50.66124200820923 seconds

    :return:
    """
    plt.xlabel('Number of Roots')
    plt.ylabel('Time (seconds)')
    x_axis = [250, 500, 750, 1000]

    htgar_y_axis = [295.95, 158.45, 136.51, 116.14]
    cumulate_y_axis = [144.14, 69.22, 59.64, 50.66]

    plt.plot(x_axis, cumulate_y_axis, linestyle='-', marker='o', color='r', label='vertical_cumulate')
    plt.plot(x_axis, htgar_y_axis, linestyle='--', marker='o', color='g', label='HTGAR')

    plt.title('ROOTS')
    plt.legend()
    plt.savefig('Plots/htgar_roots.png', format="png",)
    plt.close()

def htgar_r_interesting_experiment_plot():

    x_axis = ['0-1', '0-2', '0-3', '0-4', '0-5', '0-6', '0-7', '0-8', '0-9', '0-10', '0-11', '0-12', '0-13',
             '0-14', '0-15', '0-16', '0-17', '0-18', '0-19', '0-20', '0-21', '0-22', '0-23', '0-24', '1-1',
             '1-2', '1-3', '1-4', '1-5', '1-6', '1-7', '1-8', '1-9', '1-10', '1-11', '1-12', '2-1', '2-2',
              '2-3', '2-4', '3-1']
    y_axis_11 = [96.87, 96.26, 95.81, 96.23, 97.07, 97.30, 97.88, 97.33, 97.09, 95.07, 97.20, 97.74, 96.73, 98.95, 94.39, 97.43, 98.00, 96.84, 95.02, 96.48, 97.84, 96.77, 97.60, 95.22, 87.14, 86.49, 88.13, 90.91, 88.46, 89.73, 91.37, 86.12, 90.18, 84.89, 88.16, 92.01, 81.66, 85.35, 85.88, 85.27, 84.50]
    y_axis_13 = [94.90, 94.07, 93.36, 93.76, 95.23, 95.62, 96.53, 95.55, 95.15, 92.00, 95.33, 96.20, 94.82, 98.21, 91.12, 95.83, 96.69, 94.80, 92.06, 94.19, 96.60, 94.65, 95.92, 91.64, 78.51, 77.00, 79.81, 84.02, 79.63, 81.42, 84.79, 76.64, 82.97, 74.77, 80.33, 85.29, 65.38, 71.95, 72.75, 71.23, 65.83]
    y_axis_15 = [92.64, 91.64, 90.67, 90.99, 93.08, 93.58, 94.98, 93.49, 92.86, 88.60, 93.00, 94.44, 92.68, 97.26, 87.65, 93.89, 95.19, 92.46, 88.79, 91.53, 95.21, 92.18, 93.90, 87.39, 68.97, 66.54, 70.46, 76.22, 70.17, 71.71, 76.71, 66.39, 74.52, 64.39, 71.56, 76.41, 49.01, 56.65, 58.54, 54.69, 46.13]
    y_axis_17 = [90.30, 89.16, 87.91, 88.12, 90.90, 91.44, 93.38, 91.20, 90.43, 85.12, 90.59, 92.56, 90.52, 96.18, 84.25, 91.98, 93.58, 90.02, 85.57, 88.77, 93.76, 89.59, 91.69, 82.95, 60.48, 56.96, 61.13, 68.63, 60.77, 62.47, 68.59, 57.30, 66.08, 54.93, 63.01, 67.32, 36.15, 43.28, 46.06, 41.90, 32.10]
    y_axis_19 = [87.97, 86.68, 85.21, 85.30, 88.63, 89.24, 91.81, 88.95, 88.07, 81.70, 88.21, 90.70, 88.30, 95.02, 80.89, 90.04, 91.89, 87.54, 82.45, 86.11, 92.31, 86.94, 89.38, 78.50, 52.75, 48.60, 52.55, 61.56, 52.40, 53.71, 60.80, 49.37, 58.27, 46.47, 55.20, 58.95, 25.31, 32.17, 35.10, 31.47, 21.39]
    y_axis_21 = [85.69, 84.37, 82.61, 82.69, 86.44, 87.15, 90.31, 86.69, 85.76, 78.56, 85.88, 88.88, 86.23, 93.85, 77.68, 88.10, 90.27, 85.06, 79.49, 83.53, 90.96, 84.41, 87.19, 74.33, 45.73, 41.51, 45.22, 55.26, 45.01, 46.14, 53.69, 42.35, 51.24, 39.12, 48.44, 51.16, 16.86, 23.45, 25.63, 22.30, 12.06]



    fig, ax = plt.subplots()
    plt.xlabel('Granulo Temporal')
    plt.ylabel('% de reglas obtenidas con respecto a R=0')
    #plt.tick_params(axis='x', which='major', labelsize=8)
    ax.set_xticklabels(x_axis, rotation='vertical', fontsize=8)
    plt.yticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])

    plt.plot(x_axis, y_axis_11,marker='None', linestyle = '--', color='r', label='R=1.1')
    plt.plot(x_axis, y_axis_13, marker='None', linestyle = '--', color='g', label='R=1.3')
    plt.plot(x_axis, y_axis_15, marker='None', linestyle = '--', color='b', label='R=1.5')
    plt.plot(x_axis, y_axis_17, marker='None', linestyle = '--', color='c', label='R=1.7')
    plt.plot(x_axis, y_axis_19, marker='None', linestyle = '--', color='m', label='R=1.9')
    plt.plot(x_axis, y_axis_21, marker='None', linestyle = '--', color='y', label='R=2.1')

    plt.title('Reglas obtenidas por granulo temporal')
    plt.legend()

    plt.savefig('Plots/HTGAR_R_interesting_rules.png', format="png", bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    #Plot experiments by key
    # parse_and_plot_files('root', 'Number of roots', [250, 500, 750, 1000], 'Time (Minutes)',  [0, 5, 10, 15, 20, 25, 30, 35])
    # parse_and_plot_files('depth_ratio', 'Depth',  [0.5, 0.75, 1, 1.5, 2], 'Time (Minutes)', [0, 5, 10, 15, 20, 25, 30, 35])
    # parse_and_plot_files('fanout', 'Fanout', [5, 7.5, 10, 15, 20, 25], 'Time (Minutes)', [0, 5, 10, 15, 20, 25, 30, 35])
    # parse_and_plot_files('item', 'Number of items (000s)', [10, 25, 50, 75, 100], 'Time (Minutes)', [0, 5, 10, 15, 20, 25, 30, 35])
    # parse_and_plot_files('support', 'Minimum Support (%)', [2,1.5,1,0.75,0.5,0.33], 'Time (Minutes)', [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65])
    # parse_and_plot_files('transaction', 'Number of transactions (millions)',
    #                      [0.1, 1, 2, 5, 10], 'Time / #Transactions (normalized)',
    # [0.6, 0.8, 1, 1.2, 1.4, 1.8, 2])
    # plot_parallel_count_efectiveness_experiment()
    # plot_parallel_rule_gen_efectiveness_experiment()
    # plot_rule_gen_by_potential_rule_size()
    #plot_htgar_roots_experiment()
    htgar_r_interesting_experiment_plot()