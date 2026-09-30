# R2C1 resource design — pending final tree certification

No real HTTP before all final gates. P2v4/P3v2/identity/NDJSONv1 unchanged. Limits are admission, not promise to accept every100k-member archive; local collect-wrapper/adapter16MiB unchanged.

## Modes/ownership
Range<=8MiB: requestedbody+sentinel, Rust64KiBbuffer, Python<=two bodycopies;32MiB+2requested memory (fixedTLS/workerheadroom raised conservatively aboveobserved23MiBRSS), nowholeobjectallocation. Download boundedHTTPwrites exactbody then sameRustlocalscanner+observer; body+report+metadata stay through stage/P2durableCOMMIT. Onlyexpected frozenjobintent/fullINPUT/adapter/providerbinding/size/digest/outputroot+verifiedCOMMIT allowsrelease. Failures/crashesretainunknown leases/roots. Remote Response countedRead→sameobserver scanner; fileNone, nevercreatesbody, dropsimagepayload. PythontrustsRustsame-readimage/wholehash (cannot independentlyre-hash discardedbytes), independentlyvalidates sidecardigest/footer/count/geometry/metadataSHA. Download independentlyaudits retainedbody/memberSHA.

Workerclosed before Pythonstage. Network+stage memoryreservation128MiBconstant, P2writer and release eachnew128MiBlease; theirconversion neveroverlapsactiveworker. Job-owned allocation differs from existingloaded interpreterRSS. No fullmemberlist/aggregateJSONRAM.

## Artifact bounds
100k AND32MiBactualserializedrecords,4096Bpath/32KiBline,footer4096B apply jointly. Arbitrary100k*4096paths notpromised. Onepath/recordatonce, actualJSONescaping included beforewrite. Typical100k observer/escapedtotal captested. JSONdeclared<=min(adapter,1MiB),aggregate32MiB checkedbeforememberread; chunk checksremainingbeforewrite. Only.jsonsidecar,noimages. Report/metadatafsynced+hashed+terminalfooter onlyaftervalidEOF/exactsize/TARclose. Onerror measuredreadcount andcompletefalsewhereEOFunknown; trustedaccountchargedbeforerejection; IPCdeathpreservesnetworkpending.

Transferdisk<=64MiB+footer4096+allocation16KiB (+objectbytesDownloadonly). Closedworkerartifactsstopgrowth. Crashresourcesquota retained,noautomaticsweep/reclaim/reset.

## Fresh production SQLite
Usejournal_mode=OFF, validatedreturn, ONLYnewdisposableprepublicationstage/spools. No rollback/WAL/subjournal and no estimatedVFSsectorbound. FailurecanleavecorruptprivateDB, neverpublishsuccessmarkerorreusepartial. ExistingdeliveredDBneverwritten. LocalDELETEunchanged.4096pages/max_page_countcoverindexes;2MiBcache/nommap. tempMEMORYNOTcapitself: fixedSQLEXPLAINnosorter stageextentindex(onemptytable), memberkeydistinct/lookup,rowskindordinal,referencejoin. Readonlystage/auditconnections same2MiBcache/nommap/queryonly. Allocationcluster<=4096gate solelyfile-roundingbound,NOTjournal-sectorproof.

Stage main128MiB+1MiBsmallallocationmargin, uniquetable/extentindexinside; nojournal/tempfile. Newproduction128MiBcanrejectlegitimatelargerstage, localdefaultunchanged. P2 EACHof2spools60MiB(page15,360),nojournal. Allobjects/tablesfragmentSHAREDexisting192MiB,audit64MiB;2*60+192+64=376MiB+roundedoverhead<unchanged1152MiB. Separatecompletedstage128 and ENTIREliveDownloadtransfercountedinphases. Legitimatedatahittinglimits failsclosed, notscalablepublication.

RowUTF8serialized512KiB. Normalbatch256KiB/1024rows; largerrowSINGLETON<=512KiB,nofalse256KiBhardclaim. fetchone avoids1024largeJSONstrings; previousyield+nextbatchcanoverlap. Parquetnodictionary/nocompression/nostatistics,rowgroups<=512/table,boundedself-generatedrowgroupverify,notread_table. Releaseownershipprojects2columns128rows. FullINPUT+<=3tinyobjectdescriptorsnotallrowaggregation.

Allfragmentwritecapsbeforewrite/globalremaining. Productionmarkersafter allfragments/references/spoolsclosed/auditfsyncclose. Prepublicationresourcefailure no validfullCOMMIT. Duringmarkersmaypartialset(inventoryrejects). Afterpublicationquota-finalizationerror mayleaveVALIDdatawithpendingquota, notcorrupt. Likewisecompletedstagewithfailedsettlecanhavestage.complete.

## Memory envelope/evidence boundaries
128MiBjobbudget: workerfixedHTTP/scanner/hash/path/record+64KiBIPC/8linequeue/stderr buffers<=16MiB plus32MiB TLS/allocator/runtimeheadroom; workerRSSobserved22-24MiB. NooverlapwithPythonconversions. JSONsource1MiB/lexicaldepth32/tokens32768beforedecode; combinedsource-stringbytes≤1MiB; conservativelyUnicode/copiedstrings16MiB,token/containerobjectpopulation16MiB. Two512KiB-boundedbatchgenerations/nativefrom_pylist copiesconservatively32MiB (encoded≠decoded),≤four2MiBSQLitecaches8MiB,boundedArrowcolumnbuffers8MiB,512groupsfixedschemafooter8MiB,nodictionary/stats/codecworkspace.40MiBremainingallocator/nativewriter/verificationheadroom. Conservativeimplementation ceilings/headroom,NOTmathematicalOSRSS theorem or totalprocessbaselinecap.

64/128image-heavy2memberfixture provesbodydecoupling,notdense100kRSS.5msdisk samplingobservedpeak,notabsolutepeak; noimagewrite+hardartifactcapsstructure independentproof. WorkerRSS excludesPython. Dense900records/28,832,400metadataB: Python/nativebaseline78,913,536,observedabsolute104,378,368,delta25,464,832B; sampleNOTuniversal128MiBproof. Rust100ktypical/escapedaggregate/32MiBJSONlimit separatelytestdensity.

## Phases/ledgerconservatism
Atransfer; B=A+129MiBstage; C=completedstage128+1152durable+ENTIREDownloadA (remoteA safelyreleasedstageexit). max(A,B,C),nothistorysum. LedgerSTILLphysical+remainingreserved dualcounts materializedbytes; NOhandoff implemented(noledgerformat/replayexpansion). Actualledgerstricterthanphysicalmodel; real1.4GiBDownloadtargetmayremainBLOCKED. No caps/root/pending reducedtomakeREADY.

lstat/nonreparse/nlink1/rootidentitychecks dependonsolewriter/jobexclusivepaths; notatomicagainsthostileexternalreplacement. Unknown/hardlinkrootsretained, primaryerrorpreserved, nestedfinallyownershipattemptsevenifmemorysettlefails. Partialunlinkretainsquotaandcanimpederetry,noautomaticreclaimclaimed. Expectedcommit callback binds frozenjobadapter/provider+stage+output/fullINPUT beforewriter; laterverifiedCOMMITmustmatch, not arbitrarysameobjectidP2selfproof.
