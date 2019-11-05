import plotly.graph_objects as go
import random

from src.utils import create_collections_dataframe, create_topics_dataframe

PLOTLY_COLOR_LIST = ['aliceblue', 'aqua', 'aquamarine',
                     'azure', 'beige', 'bisque', 'black', 'blanchedalmond',
                     'blue', 'blueviolet', 'brown', 'burlywood', 'cadetblue',
                     'chartreuse', 'chocolate', 'coral', 'cornflowerblue',
                     'cornsilk', 'crimson', 'cyan', 'darkblue', 'darkcyan',
                     'darkgoldenrod', 'darkgray', 'darkgrey', 'darkgreen',
                     'darkkhaki', 'darkmagenta', 'darkolivegreen',
                     'darkorange', 'darkorchid', 'darkred', 'darksalmon',
                     'darkseagreen', 'darkslateblue', 'darkslategray',
                     'darkslategrey', 'darkturquoise', 'darkviolet',
                     'deeppink', 'deepskyblue', 'dimgray', 'dimgrey',
                     'dodgerblue', 'firebrick', 'forestgreen',
                     'fuchsia', 'gainsboro', 'gold',
                     'goldenrod', 'gray', 'grey', 'green', 'greenyellow',
                     'honeydew', 'hotpink', 'indianred', 'indigo', 'ivory',
                     'khaki', 'lavender', 'lavenderblush', 'lawngreen',
                     'lemonchiffon', 'lightblue', 'lightcoral', 'lightcyan',
                     'lightgoldenrodyellow', 'lightgray', 'lightgrey',
                     'lightgreen', 'lightpink', 'lightsalmon',
                     'lightseagreen', 'lightskyblue', 'lightslategray',
                     'lightslategrey', 'lightsteelblue', 'lightyellow',
                     'lime', 'limegreen', 'linen', 'magenta', 'maroon',
                     'mediumaquamarine', 'mediumblue', 'mediumorchid',
                     'mediumpurple', 'mediumseagreen', 'mediumslateblue',
                     'mediumspringgreen', 'mediumturquoise',
                     'mediumvioletred', 'midnightblue', 'mintcream',
                     'mistyrose', 'moccasin', 'navy',
                     'oldlace', 'olive', 'olivedrab', 'orange', 'orangered',
                     'orchid', 'palegoldenrod', 'palegreen',
                     'paleturquoise', 'palevioletred', 'papayawhip',
                     'peachpuff', 'peru', 'pink', 'plum', 'powderblue',
                     'purple', 'red', 'rosybrown', 'royalblue', 'saddlebrown',
                     'salmon', 'sandybrown', 'seagreen', 'seashell', 'sienna',
                     'silver', 'skyblue', 'slateblue', 'slategray',
                     'slategrey', 'snow', 'springgreen', 'steelblue', 'tan',
                     'teal', 'thistle', 'tomato', 'turquoise', 'violet',
                     'wheat', 'yellow', 'yellowgreen']


def create_collection_figure_trace(*args, **kwargs):
    traces = []

    if len(args) == 1:
        df = create_collections_dataframe(topic=args[0], db=kwargs['db'])
        title_text = f'time chart for {args[0]}'.title()

    if len(args) > 1:
        df = create_topics_dataframe(*args, db=kwargs['db'])
        title_text = f'time chart for topics {", ".join(args)}'.title()

    # get all column names except the date column
    column_name_list = df.columns[1:]
    color_indexes = random.sample(range(len(PLOTLY_COLOR_LIST)),
                                  len(column_name_list))

    for color_index, column_name in zip(color_indexes, column_name_list):
        trace = go.Scatter(x=df.date, y=df[column_name],
                           name=column_name,
                           line_color=PLOTLY_COLOR_LIST[color_index])

        traces.append(trace)

    return traces, title_text
