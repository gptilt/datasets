modules = []
jobs = []
schedules = []
sensors = []
resources = {}

try:
    import ds_chatbot

    modules.append(ds_chatbot)
    jobs.extend(ds_chatbot.jobs)
    resources.update(ds_chatbot.resources)
    schedules.extend(ds_chatbot.schedules)
    sensors.extend(ds_chatbot.sensors)
except ImportError:
    pass