package Nifi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import Utils.Utils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
public class NifiFlow {
    private final String templateFile;
    private final String apiUrl;
    private String templateId;
    private String processorGroupId;
    private String cachedRootProcessGroupId = "";
    private List<NifiControllerService> controllerServiceIds;


    public NifiFlow(String file_name, String apiUrl) {
        this.templateFile = file_name;
        this.apiUrl = apiUrl;
        controllerServiceIds = new ArrayList<>();
    }

    //********* metodi per ottenere info sui componenti fondamentali di NIFI *********
    public String getRootProcessGroupId() {
        if (!cachedRootProcessGroupId.isEmpty()) {
            return cachedRootProcessGroupId;
        }
        //recupera l'ID
        JSONObject response = NifiREST.nifiGET(apiUrl + "process-groups/root/");
        if (response != null && response.has("id")) {
            cachedRootProcessGroupId = response.getString("id");
        }

        return cachedRootProcessGroupId;
    }

    public List<NifiControllerService> getControllerServices(String processGroup) {
        String url = apiUrl + "flow/process-groups/" + processGroup + "/controller-services";
        List<NifiControllerService> controllerServices = new ArrayList<>();
        JSONObject controllerServicesJSON = NifiREST.nifiGET(url);

        //controlla se ci sono controller services
        if (controllerServicesJSON != null && controllerServicesJSON.has("controllerServices")) {
            JSONArray controllerServicesArray = controllerServicesJSON.getJSONArray("controllerServices");

            for (int i = 0; i < controllerServicesArray.length(); i++) {
                JSONObject controllerService = controllerServicesArray.getJSONObject(i);
                JSONObject revision = controllerService.getJSONObject("revision");
                JSONObject component = controllerService.getJSONObject("component");
                String id = controllerService.getString("id");
                String state = component.getString("state");
                int version = revision.getInt("version");
                //crea un'istanza di NifiControllerService e aggiungila alla lista
                controllerServices.add(new NifiControllerService(id, state,version));
            }
        }
        return controllerServices;
    }

    public List<String> getProcessorGroups() {
        String rootProcessGroupId = getRootProcessGroupId();
        String url = apiUrl + "process-groups/" + rootProcessGroupId + "/process-groups";
        JSONObject response = NifiREST.nifiGET(url);

        if (response != null && response.has("processGroups")) {
            JSONArray groups = response.getJSONArray("processGroups");
            return Utils.extractIdsFromJsonArray(groups);
        } else {
            return Collections.emptyList();
        }
    }

    public List<String> getProcessors(String processGroup) {
        String url = apiUrl + "process-groups/" + processGroup + "/processors";
        JSONObject response = NifiREST.nifiGET(url);

        if (response != null && response.has("processors")) {
            JSONArray processes = response.getJSONArray("processors");
            return Utils.extractIdsFromJsonArray(processes);
        } else {
            return Collections.emptyList();
        }
    }

    //*****metodi per attendere la fine di certi eventi******
    public int countEnabledControllerServices() {
        List<NifiControllerService> controllerServiceInfoList = getControllerServices(processorGroupId);
        return (int) controllerServiceInfoList.stream().filter(n -> n.getState().equals("ENABLED")).count();
    }

    private void waitUntilAllControllerServicesEnabled() {
        String processorGroup = processorGroupId;
        //String processorGroup = getProcessorGroup();
        if (processorGroup != null) {
            List<NifiControllerService> controllerServices = getControllerServices(processorGroup);
            int totalControllerServicesCount = controllerServices.size();
            int enabledServicesCount;
            do {
                enabledServicesCount = countEnabledControllerServices();
                if (enabledServicesCount != totalControllerServicesCount) {
                    try {
                        Thread.sleep(500);
                        System.out.println("Waiting until all controller services are enabled...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (enabledServicesCount != totalControllerServicesCount);
        }
    }


    public int countRunningProcessors() {
        if (getProcessorGroups().isEmpty()) {
            return 0;
        }

        String processorGroupUrl = apiUrl + "flow/process-groups/" + processorGroupId;
        JSONObject processGroupInfo= NifiREST.nifiGET(processorGroupUrl);

        if (processGroupInfo != null && processGroupInfo.has("processGroupFlow")) {
            JSONObject processGroupFlow = processGroupInfo.getJSONObject("processGroupFlow");
            if (processGroupFlow.has("flow")) {
                JSONArray processors = processGroupFlow.getJSONObject("flow").getJSONArray("processors");
                return (int) IntStream.range(0, processors.length())
                        .mapToObj(processors::getJSONObject)
                        .map(processor -> processor.getJSONObject("component").getString("state"))
                        .filter("RUNNING"::equals)
                        .count();
            }
        }
        return 0;
    }

    private void waitUntilAllProcessRunning() {
        //String processorGroup = getProcessorGroup();
        String processorGroup = processorGroupId;
        if (processorGroup != null) {
            List<String> processors = getProcessors(processorGroup);
            int totalProcessors = processors.size();
            int runningProcessorCount;
            do {
                runningProcessorCount = countRunningProcessors();
                if (runningProcessorCount != totalProcessors) {
                    try {
                        Thread.sleep(500);
                        System.out.println("Waiting until all " + runningProcessorCount + "/" + totalProcessors + " processors are running...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (runningProcessorCount != totalProcessors);
        }
    }

    private void waitUntilAllProcessStopped() {
        int runningProcessorCount;
        do {
            runningProcessorCount = countRunningProcessors();
            if (runningProcessorCount != 0) {
                try {
                    Thread.sleep(500);
                    System.out.println("Waiting until all processors are stopped...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (runningProcessorCount != 0);
    }

    private void waitForAllControllerServicesDisabled() {
        int enabledControllerServicesCount;
        do {
            enabledControllerServicesCount = countEnabledControllerServices();
            System.out.println("numberControllerServicesRunning: " + enabledControllerServicesCount);

            if (enabledControllerServicesCount != 0) {
                try {
                    Thread.sleep(500);
                    System.out.println("Waiting until all controller services are disabled...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (enabledControllerServicesCount != 0);
    }

    //*******altri metodi utili per la gestione del flusso in NIFI*******+
    public boolean terminateThreadsOfProcessorGroup(String processorGroupId) {
        List<String> processorIds = getProcessors(processorGroupId);
        boolean allTerminatedSuccessfully = true;

        for (String processorId : processorIds) {
            String terminateThreadsUrl = apiUrl + "processors/" + processorId + "/threads";
            boolean terminated = NifiREST.deleteNifi(terminateThreadsUrl, false);
            allTerminatedSuccessfully = allTerminatedSuccessfully && terminated;
            System.out.println("Terminated threads of processor " + processorId);
        }

        return allTerminatedSuccessfully;
    }

    public boolean emptyQueues(String processingGroupId) {
        //ottieni l'id delle connessioni
        String connectionsUrl = apiUrl + "process-groups/" + processingGroupId + "/connections";
        JSONObject connectionsResponse = NifiREST.nifiGET(connectionsUrl);

        if (connectionsResponse!= null && connectionsResponse.has("connections")) {
            JSONArray connections = connectionsResponse.getJSONArray("connections");
            for (int i = 0; i < connections.length(); i++) {
                JSONObject connection = connections.getJSONObject(i);
                String connectionId = connection.getString("id");
                //elimina i file di flusso
                String dropFlowFilesUrl = apiUrl + "flowfile-queues/" + connectionId + "/drop-requests";
                NifiREST.postRequest(dropFlowFilesUrl, null);
            }
            return true;
        }
        return false;
    }

    //****** metodi relativi allo STOP **********
    public boolean stopAllControllerServices() {
        for (NifiControllerService controllerService : controllerServiceIds) {
            System.out.println("Stopping controller service: " + controllerService.getId());

            String url = apiUrl + "controller-services/" + controllerService.getId() + "/run-status";
            String json = "{\n" +
                    "  \"revision\": {\n" +
                    "    \"version\": " + controllerService.getVersion() + "\n" +
                    "  },\n" +
                    "  \"id\": \"" + controllerService.getId() + "\",\n" +
                    "  \"state\": \"DISABLED\",\n" +
                    "  \"disconnectedNodeAcknowledged\": false,\n" +
                    "  \"uiOnly\": false\n" +
                    "}";

            boolean requestSuccess = NifiREST.nifiPUT(url, json);

            if (requestSuccess) {
                controllerService.incrementVersion();
            } else {
                System.out.println("Errore nella richiesta PUT per disabilitare il servizio: " + controllerService.getId());
                return false; // Interrompi immediatamente se un servizio di controller non si ferma
            }
        }
        return true; // Se tutti i servizi di controller sono stati fermati con successo, restituisci true
    }

    public boolean stop() {

        //ferma i gruppi di processi
        String s = apiUrl + "flow/process-groups/" + processorGroupId;
        String json = "{\n" +
                "    \"id\": \"" + processorGroupId + "\",\n" +
                "    \"state\": \"" + "STOPPED" + "\",\n" +
                "    \"disconnectedNodeAcknowledged\": false\n" +
                "}";
        boolean stoppedProcessGroups = NifiREST.nifiPUT(s, json);
        waitUntilAllProcessStopped();

        //ferma tutti i servizi di controller
        boolean stoppedServices = stopAllControllerServices();
        waitForAllControllerServicesDisabled();

        //termina i thread del gruppo di processori
        boolean terminatedThreads = terminateThreadsOfProcessorGroup(processorGroupId);

        //svuota le code
        boolean emptied = emptyQueues(processorGroupId);
        boolean isStopped = terminatedThreads && stoppedServices && stoppedProcessGroups && emptied;

        System.out.println("Stop operation successful: " + isStopped);

        return isStopped;
    }

    //****** metodi relativi alla REMOVE **********
    public boolean remove() {
        //attendi che la coda si svuoti
        try {
            Thread.sleep(500);
            System.out.println("Waiting before removing...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String templateToRemove = apiUrl + "templates/" + templateId;
        boolean isTemplateDeleted =NifiREST.deleteNifi(templateToRemove, true);

        //rimuovi il gruppo di processi del template
        List<String> processorGroupIds = getProcessorGroups();
        boolean areQueuesEmptied = emptyQueues(processorGroupId);

        //rimuovi tutti i gruppi di processori
        boolean areAllGroupsDeleted = true;
        for (String processorGroupId : processorGroupIds) {
            String processGroupUrl = apiUrl + "process-groups/" + processorGroupId + "?version=0";
            boolean isGroupDeleted = NifiREST.deleteNifi(processGroupUrl, true);
            areAllGroupsDeleted &= isGroupDeleted;
        }
        System.out.println("Removed process groups: " + processorGroupIds.size());

        return isTemplateDeleted && areQueuesEmptied && areAllGroupsDeleted;
    }

    public boolean removeTemplates() {
        String templatesUrl = apiUrl + "flow/templates";
        JSONObject templatesResponse = NifiREST.nifiGET(templatesUrl);
        System.out.println("templates: "+ templatesResponse);
        boolean allTemplatesRemoved = true;
        if (templatesResponse != null && templatesResponse.has("templates")) {
            JSONArray templateList = templatesResponse.getJSONArray("templates");
            for (int i = 0; i < templateList.length(); i++) {
                JSONObject template = templateList.getJSONObject(i);
                String templateId = template.getString("id");
                String deleteTemplateUrl = apiUrl + "templates/" + templateId;
                allTemplatesRemoved = allTemplatesRemoved && NifiREST.deleteNifi(deleteTemplateUrl, false);
            }
        }
        return allTemplatesRemoved;
    }

    //****** metodi relativi al RUN **********
    public boolean runControllerService(NifiControllerService controllerService) {
        String url = apiUrl + "controller-services/" + controllerService.getId() + "/run-status";
        String json = "{\n" +
                "  \"revision\": {\n" +
                "    \"version\": " + controllerService.getVersion() + "\n" +
                "  },\n" +
                " \"id\": \"" + controllerService.getId() + "\",\n" + // Assicurati che l'ID sia racchiuso tra virgolette
                " \"state\": \"ENABLED\",\n" +
                "  \"disconnectedNodeAcknowledged\": false,\n" +
                "  \"uiOnly\": false\n" +
                "}";
        boolean requestSuccess = NifiREST.nifiPUT(url, json);

        if (requestSuccess) {
            controllerService.incrementVersion();
            return true;
        } else {
            System.out.println("Error in PUT request");
            return false;
        }
    }

    public boolean runAllControllerServices() {
        for (NifiControllerService ncs : controllerServiceIds) {
            System.out.println("Running controller service: " + ncs.getId());
            if (!runControllerService(ncs)) {
                return false; //interrompi il ciclo e restituisci false se il servizio di controller fallisce
            }
        }
        return true; //se tutti i servizi di controller sono stati eseguiti con successo, restituisci true
    }

    public boolean run() {
        runAllControllerServices();
        waitUntilAllControllerServicesEnabled();
        //boolean running = setRunStatusOfProcessorGroup(processorGroupId, "RUNNING");
        String s = apiUrl + "flow/process-groups/" + processorGroupId;
        String json = "{\n" +
                "    \"id\": \"" + processorGroupId + "\",\n" +
                "    \"state\": \"" + "RUNNING" + "\",\n" +
                "    \"disconnectedNodeAcknowledged\": false\n" +
                "}";
        boolean running = NifiREST.nifiPUT(s, json);
        waitUntilAllProcessRunning();
        return running;
    }

    //******metodi per caricare e istanziare i template********
    public String uploadTemplate(String file) {
        String url = apiUrl + "process-groups/root/templates/upload";
        String response = NifiREST.nifiPOST(file, url);

        if (response != null) {
            System.out.println("Response: " + response);

            Matcher m = Pattern.compile("<id>(.*)</id>").matcher(response);
            if (m.find()) {
                return m.group(1);
            }
        } else {
            System.out.println("Request failed.");
        }

        return null;
    }

    public String instantiateTemplate(String templateId) {
        String rootProcessGroup = getRootProcessGroupId();
        String url = apiUrl + "process-groups/" + rootProcessGroup + "/template-instance";
        String jsonInputString = "{\n" +
                "    \"originX\": 2.0,\n" +
                "    \"originY\": 3.0,\n" +
                "    \"templateId\": \"" + templateId + "\"\n" +
                "}";

        //invia una richiesta POST per istanziare il template
        JSONObject response = NifiREST.postRequest(url, jsonInputString);

        System.out.println("Response JSON: " + response.toString());

        //elabora la risposta per estrarre l'ID del gruppo di processori
        try {
            JSONObject flow = response.getJSONObject("flow");
            JSONArray processGroups = flow.getJSONArray("processGroups");

            if (processGroups.length() > 0) {
                JSONObject firstProcessGroup = processGroups.getJSONObject(0);
                return firstProcessGroup.getString("id");
            } else {
                System.err.println("No process groups instantiated.");
                return null;
            }
        } catch (JSONException e) {
            System.err.println("Error processing JSON: " + e.getMessage());
            return null;
        }
    }
    public void uploadAndInstantiateTemplate() {

        boolean templatesRemoved = removeTemplates();
        if (!templatesRemoved) {
            System.out.println("Failed to remove templates");

        }
        //upload del template
        System.out.println("Uploading template " + templateFile);
        String templateId = uploadTemplate(templateFile);
        if (templateId != null) {
            this.templateId = templateId;
            System.out.println("templateId = " + this.templateId);

            //istanzio un processGroup dal template
            System.out.println("Instantiating template " + this.templateFile);
            String processorGroupId = instantiateTemplate(templateId);
            if (processorGroupId != null) {
                System.out.println("processorGroupId = " + processorGroupId);
                this.processorGroupId = processorGroupId;

                System.out.println("Getting controller services");
                controllerServiceIds = getControllerServices(processorGroupId);
                for (NifiControllerService service : controllerServiceIds) {
                    System.out.println(service); //stampa l'oggetto ControllerService
                }

            }
        }
    }


}
