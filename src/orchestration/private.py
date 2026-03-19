modules = []
jobs = []
schedules = []
sensors = []
resources = {}

try:
    import ds_chatbot

    modules.extend(ds_chatbot.modules)
    jobs.extend(ds_chatbot.jobs)
    schedules.extend(ds_chatbot.schedules)
    sensors.extend(ds_chatbot.sensors)
    resources.update(ds_chatbot.resources)
except ImportError:
    pass