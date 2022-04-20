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
    plt.title('Parallelization')
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
    plot_parallel_count_efectiveness_experiment()