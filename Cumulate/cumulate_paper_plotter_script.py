import os
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.legend import Legend
from matplotlib.offsetbox import AnchoredText
from matplotlib.pyplot import figure


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

def plot_rule_gen_python():
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
    plt.xlabel('Rules')
    plt.ylabel('Time (seconds)')

    x_axis = [100000, 500000, 1000000, 2000000, 5000000, 10000000, 20000000]
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
    plot_rule_gen_python()