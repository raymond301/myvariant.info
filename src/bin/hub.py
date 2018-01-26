#!/usr/bin/env python

import asyncio, asyncssh, sys, os, copy
import concurrent.futures
import multiprocessing_on_dill
concurrent.futures.process.multiprocessing = multiprocessing_on_dill
from functools import partial

from collections import OrderedDict

import config, biothings
biothings.config_for_app(config)

import logging
# shut some mouths...
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("boto").setLevel(logging.ERROR)

logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)

from biothings.utils.manager import JobManager
loop = asyncio.get_event_loop()
job_manager = JobManager(loop,num_workers=config.HUB_MAX_WORKERS,
                      num_threads=config.HUB_MAX_THREADS,
                      max_memory_usage=config.HUB_MAX_MEM_USAGE)

import hub.dataload
import biothings.hub.dataload.uploader as uploader
import biothings.hub.dataload.dumper as dumper
import biothings.hub.dataload.source as source
import biothings.hub.databuild.builder as builder
import biothings.hub.databuild.differ as differ
import biothings.hub.databuild.syncer as syncer
import biothings.hub.dataindex.indexer as indexer
import biothings.hub.datainspect.inspector as inspector
#import biothings.hub.dataindex.idcache as idcache
from hub.databuild.builder import MyVariantDataBuilder
from hub.databuild.mapper import TagObserved
from hub.dataindex.indexer import VariantIndexer
from biothings.utils.hub import schedule, pending, done, CompositeCommand, \
                                start_server, HubShell, CommandDefinition

shell = HubShell()

# will check every 10 seconds for sources to upload
upload_manager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=job_manager)
dmanager = dumper.DumperManager(job_manager=job_manager)
smanager = source.SourceManager(hub.dataload.__sources_dict__,dmanager,upload_manager)

dmanager.schedule_all()
upload_manager.poll('upload',lambda doc: shell.launch(partial(upload_manager.upload_src,doc["_id"])))

# deal with 3rdparty datasources
import biothings.hub.dataplugin.assistant as assistant
from biothings.hub.dataplugin.manager import DataPluginManager
dp_manager = DataPluginManager(job_manager=job_manager)
assistant_manager = assistant.AssistantManager(data_plugin_manager=dp_manager,
                                               dumper_manager=dmanager,
                                               uploader_manager=upload_manager,
                                               job_manager=job_manager)
# register available plugin assitant
assistant_manager.configure()
# load existing plugins
assistant_manager.load()

observed = TagObserved(name="observed")
build_manager = builder.BuilderManager(
        builder_class=partial(MyVariantDataBuilder,mappers=[observed]),
        job_manager=job_manager)
build_manager.configure()

differ_manager = differ.DifferManager(job_manager=job_manager)
differ_manager.configure([differ.ColdHotSelfContainedJsonDiffer,differ.SelfContainedJsonDiffer])

inspector = inspector.InspectorManager(upload_manager=upload_manager,
                                       build_manager=build_manager,
                                       job_manager=job_manager)

from biothings.hub.databuild.syncer import ThrottledESColdHotJsonDiffSelfContainedSyncer, ThrottledESJsonDiffSelfContainedSyncer, \
                                           ESColdHotJsonDiffSelfContainedSyncer, ESJsonDiffSelfContainedSyncer
syncer_manager = syncer.SyncerManager(job_manager=job_manager)
syncer_manager.configure(klasses=[ESColdHotJsonDiffSelfContainedSyncer,ESJsonDiffSelfContainedSyncer])

syncer_manager_prod = syncer.SyncerManager(job_manager=job_manager)
syncer_manager_prod.configure(klasses=[partial(ThrottledESColdHotJsonDiffSelfContainedSyncer,config.MAX_SYNC_WORKERS),
                                       partial(ThrottledESJsonDiffSelfContainedSyncer,config.MAX_SYNC_WORKERS)])

index_manager = indexer.IndexerManager(job_manager=job_manager)
pindexer = partial(VariantIndexer,es_host=config.ES_HOST,
                   timeout=config.ES_TIMEOUT,max_retries=config.ES_MAX_RETRY,
                   retry_on_timeout=config.ES_RETRY)
#pidcacher = partial(idcache.RedisIDCache,connection_params=config.REDIS_CONNECTION_PARAMS)
#coldhot_pindexer = partial(indexer.ColdHotIndexer,pidcacher=pidcacher,es_host=config.ES_HOST)
index_manager.configure([{"default":pindexer}])

import biothings.utils.mongo as mongo
def snpeff(build_name=None,sources=[], force_use_cache=True):
    """
    Shortcut to run snpeff on all sources given a build_name
    or a list of source names will process sources one by one
    Since it's particularly useful when snpeff data needs reprocessing

    force_use_cache=True is used to make sure all cache files are used to
    speed up, while source is actually being postprocessed. We're assuming
    data hasn't changed and there's no new _ids since the last time source
    was processed.
    """
    if build_name:
        sources = mongo.get_source_fullnames(build_manager.list_sources(build_name))
    else:
        sources = mongo.get_source_fullnames(sources)
    # remove any snpeff related collection
    sources = [src for src in sources if not src.startswith("snpeff")]
    config.logger.info("Sequentially running snpeff on %s" % repr(sources))
    @asyncio.coroutine
    def do(srcs):
        for src in srcs:
            config.logger.info("Running snpeff on '%s'" % src)
            job = upload_manager.upload_src(src,steps="post",force_use_cache=force_use_cache)
            yield from asyncio.wait(job)
    task = asyncio.ensure_future(do(sources))
    return task

def rebuild_cache(build_name=None,sources=None,target=None,force_build=False):
    """Rebuild cache files for all sources involved in build_name, as well as 
    the latest merged collection found for that build"""
    if build_name:
        sources = mongo.get_source_fullnames(build_manager.list_sources(build_name))
        target = mongo.get_latest_build(build_name)
    elif sources:
        sources = mongo.get_source_fullnames(sources)
    if not sources and not target:
        raise Exception("No valid sources found")

    def rebuild(col):
        cur = mongo.id_feeder(col,batch_size=10000,logger=config.logger,force_build=force_build)
        [i for i in cur] # just iterate

    @asyncio.coroutine
    def do(srcs,tgt):
        pinfo = {"category" : "cache",
                "source" : None,
                "step" : "rebuild",
                "description" : ""}
        config.logger.info("Rebuild cache for sources: %s, target: %s" % (srcs,tgt))
        for src in srcs:
            # src can be a full name (eg. clinvar.clinvar_hg38) but id_feeder knows only name (clinvar_hg38)
            if "." in src:
                src = src.split(".")[1]
            config.logger.info("Rebuilding cache for source '%s'" % src)
            col = mongo.get_src_db()[src]
            pinfo["source"] = src
            job = yield from job_manager.defer_to_thread(pinfo, partial(rebuild,col))
            yield from job
            config.logger.info("Done rebuilding cache for source '%s'" % src)
        if tgt:
            config.logger.info("Rebuilding cache for target '%s'" % tgt)
            col = mongo.get_target_db()[tgt]
            pinfo["source"] = tgt
            job = job_manager.defer_to_thread(pinfo, partial(rebuild,col))
            yield from job

    task = asyncio.ensure_future(do(sources,target))
    return task

COMMANDS = OrderedDict()
# getting info
COMMANDS["source_info"] = CommandDefinition(command=smanager.get_source,tracked=False)
# dump commands
COMMANDS["dump"] = dmanager.dump_src
COMMANDS["dump_all"] = dmanager.dump_all
# upload commands
COMMANDS["upload"] = upload_manager.upload_src
COMMANDS["upload_all"] = upload_manager.upload_all
COMMANDS["snpeff"] = snpeff
COMMANDS["rebuild_cache"] = rebuild_cache
# building/merging
COMMANDS["whatsnew"] = build_manager.whatsnew
COMMANDS["lsmerge"] = build_manager.list_merge
COMMANDS["rmmerge"] = build_manager.delete_merge
COMMANDS["merge"] = build_manager.merge
COMMANDS["premerge"] = partial(build_manager.merge,steps=["merge","metadata"])
COMMANDS["es_sync_hg19_test"] = partial(syncer_manager.sync,"es",target_backend=config.ES_TEST_HG19)
COMMANDS["es_sync_hg38_test"] = partial(syncer_manager.sync,"es",target_backend=config.ES_TEST_HG38)
COMMANDS["es_sync_hg19_prod"] = partial(syncer_manager_prod.sync,"es",target_backend=config.ES_PROD_HG19)
COMMANDS["es_sync_hg38_prod"] = partial(syncer_manager_prod.sync,"es",target_backend=config.ES_PROD_HG38)
COMMANDS["es_prod"] = {"hg19":config.ES_PROD_HG19,"hg38":config.ES_PROD_HG38}
COMMANDS["es_test"] = {"hg19":config.ES_TEST_HG19,"hg38":config.ES_TEST_HG38}
# diff
COMMANDS["diff_demo"] = partial(differ_manager.diff,differ.SelfContainedJsonDiffer.diff_type)
COMMANDS["diff_hg38"] = partial(differ_manager.diff,differ.SelfContainedJsonDiffer.diff_type)
COMMANDS["diff_hg19"] = partial(differ_manager.diff,differ.ColdHotSelfContainedJsonDiffer.diff_type)
COMMANDS["report"] = differ_manager.diff_report
COMMANDS["release_note"] = differ_manager.release_note
COMMANDS["publish_diff_hg19"] = partial(differ_manager.publish_diff,config.S3_APP_FOLDER + "-hg19")
COMMANDS["publish_diff_hg38"] = partial(differ_manager.publish_diff,config.S3_APP_FOLDER + "-hg38")
# indexing commands
COMMANDS["index"] = index_manager.index
COMMANDS["snapshot"] = index_manager.snapshot
COMMANDS["snapshot_demo"] = partial(index_manager.snapshot,repository=config.SNAPSHOT_REPOSITORY + "-demo")
COMMANDS["publish_snapshot_hg19"] = partial(index_manager.publish_snapshot,config.S3_APP_FOLDER + "-hg19")
COMMANDS["publish_snapshot_hg38"] = partial(index_manager.publish_snapshot,config.S3_APP_FOLDER + "-hg38")
# inspector
COMMANDS["inspect"] = inspector.inspect
# demo
COMMANDS["publish_diff_demo_hg19"] = partial(differ_manager.publish_diff,config.S3_APP_FOLDER + "-demo_hg19",
                                        s3_bucket=config.S3_DIFF_BUCKET + "-demo")
COMMANDS["publish_diff_demo_hg38"] = partial(differ_manager.publish_diff,config.S3_APP_FOLDER + "-demo_hg38",
                                        s3_bucket=config.S3_DIFF_BUCKET + "-demo")
COMMANDS["publish_snapshot_demo_hg19"] = partial(index_manager.publish_snapshot,config.S3_APP_FOLDER + "-demo_hg19",
                                                                                ro_repository=config.READONLY_SNAPSHOT_REPOSITORY + "-demo")
COMMANDS["publish_snapshot_demo_hg38"] = partial(index_manager.publish_snapshot,config.S3_APP_FOLDER + "-demo_hg38",
                                                                                ro_repository=config.READONLY_SNAPSHOT_REPOSITORY + "-demo")
# data plugins
COMMANDS["register_url"] = partial(assistant_manager.register_url)
COMMANDS["unregister_url"] = partial(assistant_manager.unregister_url)

# admin/advanced
EXTRA_NS = {
        "dm" : CommandDefinition(command=dmanager,tracked=False),
        "dpm" : CommandDefinition(command=dp_manager,tracked=False),
        "am" : CommandDefinition(command=assistant_manager,tracked=False),
        "um" : CommandDefinition(command=upload_manager,tracked=False),
        "bm" : CommandDefinition(command=build_manager,tracked=False),
        "dim" : CommandDefinition(command=differ_manager,tracked=False),
        "sm" : CommandDefinition(command=syncer_manager,tracked=False),
        "im" : CommandDefinition(command=index_manager,tracked=False),
        "jm" : CommandDefinition(command=job_manager,tracked=False),
        "ism" : CommandDefinition(command=inspector,tracked=False),
        "mongo_sync" : CommandDefinition(command=partial(syncer_manager.sync,"mongo"),tracked=False),
        "es_sync" : CommandDefinition(command=partial(syncer_manager.sync,"es"),tracked=False),
        "loop" : CommandDefinition(command=loop,tracked=False),
        "pqueue" : CommandDefinition(command=job_manager.process_queue,tracked=False),
        "tqueue" : CommandDefinition(command=job_manager.thread_queue,tracked=False),
        "g" : CommandDefinition(command=globals(),tracked=False),
        "sch" : CommandDefinition(command=partial(schedule,loop),tracked=False),
        "top" : CommandDefinition(command=job_manager.top,tracked=False),
        "pending" : CommandDefinition(command=pending,tracked=False),
        "done" : CommandDefinition(command=done,tracked=False),
        # required by API only (just fyi)
        "builds" : CommandDefinition(command=build_manager.build_info,tracked=False),
        "build" : CommandDefinition(command=lambda id,*args,**kwargs: build_manager.build_info(id=id,*args,**kwargs),tracked=False),
        "job_info" : CommandDefinition(command=job_manager.job_info,tracked=False),
        "dump_info" : CommandDefinition(command=dmanager.dump_info,tracked=False),
        "upload_info" : CommandDefinition(command=upload_manager.upload_info,tracked=False),
        "build_config_info" : CommandDefinition(command=build_manager.build_config_info,tracked=False),
        "commands" : CommandDefinition(command=shell.command_info,tracked=False),
        "command" : CommandDefinition(command=lambda id,*args,**kwargs: shell.command_info(id=id,*args,**kwargs),tracked=False),
        "sources" : CommandDefinition(command=smanager.get_sources,tracked=False),
}

import tornado.web
from biothings.hub.api import generate_api_routes, EndpointDefinition

API_ENDPOINTS = {
        # extra commands for API
        "builds" : EndpointDefinition(name="builds",method="get"),
        "build" : [EndpointDefinition(method="get",name="build"),
                   EndpointDefinition(method="delete",name="rmmerge"),
                   EndpointDefinition(method="post",name="merge",)],
        "job_manager" : EndpointDefinition(name="job_info",method="get"),
        "dump_manager": EndpointDefinition(name="dump_info", method="get"),
        "upload_manager" : EndpointDefinition(name="upload_info",method="get"),
        "build_manager" : EndpointDefinition(name="build_config_info",method="get"),
        "commands" : EndpointDefinition(name="commands",method="get"),
        "command" : EndpointDefinition(name="command",method="get"),
        "sources" : EndpointDefinition(name="sources",method="get"),
        "source" : [EndpointDefinition(name="source_info",method="get"),
                    EndpointDefinition(name="dump",method="put",suffix="dump"),
                    EndpointDefinition(name="upload",method="put",suffix="upload")],
        "inspect" : EndpointDefinition(name="inspect",method="put",force_bodyargs=True),
        "dataplugin/register_url" : EndpointDefinition(name="register_url",method="post",force_bodyargs=True),
        "dataplugin/unregister_url" : EndpointDefinition(name="unregister_url",method="delete",force_bodyargs=True)
        }

shell.set_commands(COMMANDS,EXTRA_NS)

settings = {'debug': True}
routes = generate_api_routes(shell, API_ENDPOINTS,settings=settings)
#routes = generate_api_routes(shell, {"source" : API_ENDPOINTS["source"]},settings=settings)
app = tornado.web.Application(routes,settings=settings)
EXTRA_NS["app"] = app

# register app into current event loop
import tornado.platform.asyncio
tornado.platform.asyncio.AsyncIOMainLoop().install()
app_server = tornado.httpserver.HTTPServer(app)
app_server.listen(config.HUB_API_PORT)
app_server.start()

server = start_server(loop,"MyVariant hub",passwords=config.HUB_PASSWD,
                      port=config.HUB_SSH_PORT,shell=shell)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()

