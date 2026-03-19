modules = []
jobs = []
schedules = []
resources = {}

try:
    import ds_chatbot

    modules.extend(ds_chatbot.modules)
    jobs.extend(ds_chatbot.jobs)
    schedules.extend(ds_chatbot.schedules)
    resources.update(ds_chatbot.resources)
except ImportError:
    pass