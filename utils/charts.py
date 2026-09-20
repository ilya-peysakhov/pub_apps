def get_strikes_attempted_chart(dates, values):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "title": {"text": "Strikes Attempted"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": dates},
        "yAxis": {"type": "value"},
        "series": [{"data": values, "type": "line", "areaStyle": {}}]
    }

def get_strike_diff_chart(dates, values):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "title": {"text": "Net Sig Strike Landed difference"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": dates},
        "yAxis": {"type": "value"},
        "series": [{"data": values, "type": "line", "areaStyle": {}}]
    }

def get_td_attempted_chart(dates, values):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "title": {"text": "Takedowns Attempted"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": dates},
        "yAxis": {"type": "value"},
        "series": [{"data": values, "type": "line", "areaStyle": {}}]
    }

def get_td_diff_chart(dates, values):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "title": {"text": "Net Takedown difference"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": dates},
        "yAxis": {"type": "value"},
        "series": [{"data": values, "type": "line", "areaStyle": {}}]
    }

def get_cum_trauma_chart(dates, values, trend_values, slope):
    return {
        "grid": {"left": "8%", "right": "8%", "bottom": "15%", "top": "15%", "containLabel": True},
        "title": {"text": "Cumulative Head Trauma with Trendline"},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": ["Cumulative Head Trauma", f"Trendline (slope = {slope:.2f} per fight)"]},
        "xAxis": {"type": "category", "data": dates, "name": "Date"},
        "yAxis": {"type": "value", "name": "Cumulative Head Trauma", "min": 0},
        "series": [
            {
                "name": "Cumulative Head Trauma",
                "type": "line",
                "areaStyle": {},
                "data": values
            },
            {
                "name": f"Trendline (slope = {slope:.2f} per fight)",
                "type": "line",
                "lineStyle": {"type": "dashed", "color": "red"},
                "itemStyle": {"color": "red"},
                "data": trend_values
            }
        ]
    }

def get_monthly_fights_chart(months, fights, events):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": ["FIGHTS", "EVENTS"]},
        "xAxis": {"type": "category", "data": months},
        "yAxis": {"type": "value"},
        "series": [
            {"name": "FIGHTS", "type": "line", "areaStyle": {}, "data": fights},
            {"name": "EVENTS", "type": "line", "areaStyle": {}, "data": events}
        ]
    }

def get_methods_pie_chart(pie_data):
    return {
        "tooltip": {"trigger": "item"},
        "legend": {"orient": "vertical", "left": "left"},
        "series": [{
            "name": "Method",
            "type": "pie",
            "radius": "60%",
            "data": pie_data,
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowOffsetX": 0,
                    "shadowColor": "rgba(0, 0, 0, 0.5)"
                }
            }
        }]
    }

def get_fight_distro_chart(fights_categories, fighter_counts):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": fights_categories, "name": "FIGHTS"},
        "yAxis": {"type": "value", "name": "FIGHTERS"},
        "series": [{"data": fighter_counts, "type": "bar"}]
    }

def get_locations_chart(locations, events):
    return {
        "grid": {"left": "15%", "right": "10%", "bottom": "15%", "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "value", "name": "EVENTS"},
        "yAxis": {"type": "category", "data": locations},
        "series": [{"data": events, "type": "bar"}]
    }

def get_methods_over_time_chart(unique_methods, months_str, series_list):
    return {
        "grid": {"left": "5%", "right": "5%", "bottom": "15%", "top": "15%", "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": unique_methods, "top": "top"},
        "xAxis": {"type": "category", "data": months_str},
        "yAxis": {"type": "value"},
        "series": series_list
    }

def get_scatter_chart(chart_metric1, chart_metric2, scatter_series_data, trendline_data, slope):
    return {
        "grid": {"left": "10%", "right": "10%", "bottom": "15%", "top": "15%", "containLabel": True},
        "tooltip": {
            "formatter": "{c}"
        },
        "legend": {"data": ["Fighters", f"Best Fit Line (slope = {slope:.2f})"]},
        "xAxis": {"type": "value", "name": chart_metric1, "scale": True},
        "yAxis": {"type": "value", "name": chart_metric2, "scale": True},
        "series": [
            {
                "name": "Fighters",
                "type": "scatter",
                "data": scatter_series_data,
                "tooltip": {
                    "formatter": "Function(params) { return params.data[2] + '<br/>' + params.seriesName + ': ' + params.data[0] + ', ' + params.data[1]; }"
                }
            },
            {
                "name": f"Best Fit Line (slope = {slope:.2f})",
                "type": "line",
                "data": trendline_data,
                "lineStyle": {"color": "red", "type": "dashed"},
                "itemStyle": {"color": "red"}
            }
        ]
    }
