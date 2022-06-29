package cmsc433.p4.actors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import cmsc433.p4.enums.*;
import cmsc433.p4.messages.*;
import cmsc433.p4.util.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.actor.AbstractActor;

public class ResourceManagerActor extends AbstractActor {
	
	private ActorRef logger;					// Actor to send logging messages to
	
	/**
	 * Props structure-generator for this class.
	 * @return  Props structure
	 */
	static Props props (ActorRef logger) {
		return Props.create(ResourceManagerActor.class, logger);
	}
	
	/**
	 * Factory method for creating resource managers
	 * @param logger			Actor to send logging messages to
	 * @param system			Actor system in which manager will execute
	 * @return					Reference to new manager
	 */
	public static ActorRef makeResourceManager (ActorRef logger, ActorSystem system) {
		ActorRef newManager = system.actorOf(props(logger));
		return newManager;
	}
	
	/**
	 * Sends a message to the Logger Actor
	 * @param msg The message to be sent to the logger
	 */
	public void log (LogMsg msg) {
		logger.tell(msg, getSelf());
	}
	
	/**
	 * Constructor
	 * 
	 * @param logger			Actor to send logging messages to
	 */
	private ResourceManagerActor(ActorRef logger) {
		super();
		this.logger = logger;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(AddRemoteManagersRequestMsg.class, this::addRemoteManagersRequest)
				.match(AddLocalUsersRequestMsg.class, this::addLocalUsersRequest)
				.match(AddInitialLocalResourcesRequestMsg.class, this::addInitialLocalResourcesRequest)
				.match(AccessRequestMsg.class, this::accessRequest)
				.match(ManagementRequestMsg.class, this::managementRequest)
				.match(AccessReleaseMsg.class, this::accessRelease)
				.match(WhoHasResourceRequestMsg.class, this::whoHasResourceRequest)
				.match(WhoHasResourceResponseMsg.class, this::whoHasResourceResponse)
				.build();
	}

	// You may want to add data structures for managing local resources and users, storing
	// remote managers, etc.
	//
	// REMEMBER:  YOU ARE NOT ALLOWED TO CREATE MUTABLE DATA STRUCTURES THAT ARE SHARED BY
	// MULTIPLE ACTORS!
	
	private HashMap<String, Resource> localResources = new HashMap<>(); 
	private HashSet<ActorRef> remoteManagers = new HashSet<>(); 
	private HashSet<ActorRef> localUsers = new HashSet<>();
	private HashMap<String, ActorRef> knownManagers = new HashMap<>(); 
	private Queue<AccessRequestMsg> accessRequestQueue = new LinkedList<>();
	private HashMap<String, List<UserAccessTuple>> userAccess = new HashMap<>();
	private HashMap<String, List<ManagementRequestMsg>> disableRequests = new HashMap<>();
	private HashMap<String, List<HashMap<Object, Integer>>> unknownResources = new HashMap<>();
	
	/* (non-Javadoc)
	 * 
	 * You must provide an implementation of the onReceive() method below.
	 * 
	 * @see akka.actor.AbstractActor#createReceive
	 */
	
	// ----------------------- Initialization Handlers ------------------------------
		
	public void addRemoteManagersRequest(AddRemoteManagersRequestMsg msg) throws Exception {		
		for (ActorRef manager : msg.getManagerList()) {
			if (!manager.equals(getSelf())){
				remoteManagers.add(manager);
			}
		}
		getSender().tell(new AddRemoteManagersResponseMsg(msg), getSelf());
	}
	
	public void addLocalUsersRequest(AddLocalUsersRequestMsg msg) throws Exception {		
		for (ActorRef user : msg.getLocalUsers()) {
			localUsers.add(user);
		}
		getSender().tell(new AddLocalUsersResponseMsg(msg), getSelf());
	}

	public void addInitialLocalResourcesRequest(AddInitialLocalResourcesRequestMsg msg) throws Exception {		
		for (Resource r : msg.getLocalResources()) {
			localResources.put(r.name, r);
			userAccess.put(r.getName(), new LinkedList<>());
			disableRequests.put(r.getName(), new LinkedList<ManagementRequestMsg>());
			r.enable();
			log(LogMsg.makeLocalResourceCreatedLogMsg(getSelf(), r.name));
			log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), r.name, ResourceStatus.ENABLED));
		}
		getSender().tell(new AddInitialLocalResourcesResponseMsg(msg), getSelf());
	}
	
	// ---------------------- Load Request Processing ---------------------------------
	
	public void accessRequest(AccessRequestMsg msg) throws Exception {		
		if (accessRequestQueue.contains(msg)) {
			accessRequestQueue.remove(msg);
		} else {
			log(LogMsg.makeAccessRequestReceivedLogMsg(msg.getReplyTo(), getSelf(), msg.getAccessRequest()));
		}
		
		if (!localResources.containsKey(msg.getAccessRequest().getResourceName())) {
			if (knownManagers.containsKey(msg.getAccessRequest().getResourceName())) {
				ActorRef forwardTo = knownManagers.get(msg.getAccessRequest().getResourceName());
				log(LogMsg.makeAccessRequestForwardedLogMsg(forwardTo, getSelf(), msg.getAccessRequest()));
				forwardTo.tell(msg, msg.getReplyTo());
			} else {
				searchForResourceRequest(msg);
			}
		} else {
			// If the resource is disabled or going to be disabled
			if (localResources.get(msg.getAccessRequest().getResourceName()).getStatus() == ResourceStatus.DISABLED
				|| (disableRequests.get(msg.getAccessRequest().getResourceName()) != null 
					&& disableRequests.get(msg.getAccessRequest().getResourceName()).size() != 0)) {
				
				log(LogMsg.makeAccessRequestDeniedLogMsg(msg.getReplyTo(), getSelf(), msg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
				msg.getReplyTo().tell(new AccessRequestDeniedMsg(msg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
			} else {
				List<UserAccessTuple> usersWaiting = userAccess.get(msg.getAccessRequest().getResourceName());
					
				boolean canAccess = true;
				
				for (int i = 0; i < usersWaiting.size(); i++) {
					UserAccessTuple tup = usersWaiting.get(i);
					if (tup.getUser().equals(msg.getReplyTo())) { // re-entrant lock
						break;
					} else { // non-reentrant lock
						if(tup.getAccessType().equals(AccessType.EXCLUSIVE_WRITE)) { // can't write while others are reading or writing
							canAccess = false;
							break;
						} else if (tup.getAccessType().equals(AccessType.CONCURRENT_READ)) { // can't read while others are writing
							if (msg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING 
								|| msg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING) {
								canAccess = false;
								break;
							}	
						}
					}
				}
				
				if (canAccess == false) {
					AccessRequestType type = msg.getAccessRequest().getType();
					if (type == AccessRequestType.CONCURRENT_READ_NONBLOCKING || type == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING) {
						log(LogMsg.makeAccessRequestDeniedLogMsg(msg.getReplyTo(), getSelf(), msg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
						msg.getReplyTo().tell(new AccessRequestDeniedMsg(msg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY), getSelf());
					} else if (type == AccessRequestType.CONCURRENT_READ_BLOCKING || type == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING) {
						accessRequestQueue.add(msg);
					}
				} else {
					AccessRequestType type = msg.getAccessRequest().getType();
					if (type == AccessRequestType.CONCURRENT_READ_BLOCKING || type == AccessRequestType.CONCURRENT_READ_NONBLOCKING) {
						
						UserAccessTuple tmp = new UserAccessTuple(msg.getReplyTo(), AccessType.CONCURRENT_READ);
						userAccess.get(msg.getAccessRequest().getResourceName()).add(tmp);
					} else if (type == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING || type == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING) {
						UserAccessTuple tmp = new UserAccessTuple(msg.getReplyTo(), AccessType.EXCLUSIVE_WRITE);
						userAccess.get(msg.getAccessRequest().getResourceName()).add(tmp);
					}
					
					log(LogMsg.makeAccessRequestGrantedLogMsg(msg.getReplyTo(), getSelf(), msg.getAccessRequest()));
					msg.getReplyTo().tell(new AccessRequestGrantedMsg(msg.getAccessRequest()), getSelf());
				}	
			}
		}
	}
	
	public void accessRelease(AccessReleaseMsg msg) throws Exception {		
		log(LogMsg.makeAccessReleaseReceivedLogMsg(msg.getSender(), getSelf(), msg.getAccessRelease()));
		
		if (!localResources.containsKey(msg.getAccessRelease().getResourceName())) {
			if (knownManagers.containsKey(msg.getAccessRelease().getResourceName())) {
				ActorRef forwardTo = knownManagers.get(msg.getAccessRelease().getResourceName());
				log(LogMsg.makeAccessReleaseForwardedLogMsg(getSelf(), forwardTo, msg.getAccessRelease()));
				forwardTo.tell(msg, msg.getSender());
			} else {
				searchForResourceRelease(msg);
			}
		} else {
			// resource is here
			List<UserAccessTuple> lst = userAccess.get(msg.getAccessRelease().getResourceName());
			Iterator<UserAccessTuple> iterator = lst.iterator();
			boolean holdsAccess = false;
			
			while(iterator.hasNext()) {
				UserAccessTuple tup = iterator.next();
				if (tup.getUser().equals(msg.getSender()) && tup.getAccessType().equals(msg.getAccessRelease().getType())) {
					holdsAccess = true;
					log(LogMsg.makeAccessReleasedLogMsg(msg.getSender(), getSelf(), msg.getAccessRelease()));
					iterator.remove();
					break;
				}
			}
			
			if (holdsAccess == false) {
				log(LogMsg.makeAccessReleaseIgnoredLogMsg(msg.getSender(), getSelf(), msg.getAccessRelease()));
			}
			
			if(lst.size() == 0) {
				if (localResources.get(msg.getAccessRelease().getResourceName()).getStatus() == ResourceStatus.ENABLED 
					&& disableRequests.containsKey(msg.getAccessRelease().getResourceName())) {
					
					for(ManagementRequestMsg disableReq : disableRequests.get(msg.getAccessRelease().getResourceName())) {
						disableReq.getReplyTo().tell(new ManagementRequestGrantedMsg(disableReq.getRequest()), getSelf());
						log(LogMsg.makeManagementRequestGrantedLogMsg(disableReq.getReplyTo(), getSelf(), disableReq.getRequest()));
						log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), msg.getAccessRelease().getResourceName(), localResources.get(msg.getAccessRelease().getResourceName()).getStatus()));
					}
				}
			}
		}
		// Now that a user relinquished their access, we can go over the queue and allow another user to gain access 
		if (accessRequestQueue.size() > 0) {
			processQueueAndGrantAccess();
		}
		
	}
	
	public void managementRequest(ManagementRequestMsg msg) throws Exception {		
		log(LogMsg.makeManagementRequestReceivedLogMsg(msg.getReplyTo(), getSelf(), msg.getRequest()));
		
		if (!localResources.containsKey(msg.getRequest().getResourceName())) { // resource not in local resources
			if (knownManagers.containsKey(msg.getRequest().getResourceName())) {
				ActorRef forwardTo = knownManagers.get(msg.getRequest().getResourceName());
				log(LogMsg.makeManagementRequestForwardedLogMsg(getSelf(), forwardTo, msg.getRequest()));
				forwardTo.tell(msg, msg.getReplyTo());
			} else {
				searchForManagerRequest(msg);
			}
		} else { // resource is in local resources
			if (msg.getRequest().getType() == ManagementRequestType.ENABLE) {
				// if status is already enabled, don't log anything. Only log if previous status is disabled
				if (localResources.get(msg.getRequest().getResourceName()).getStatus() == ResourceStatus.DISABLED) {
					localResources.get(msg.getRequest().getResourceName()).enable();
					log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), msg.getRequest().getResourceName(), ResourceStatus.ENABLED));
				}
				log(LogMsg.makeManagementRequestGrantedLogMsg(msg.getReplyTo(), getSelf(), msg.getRequest()));
				msg.getReplyTo().tell(new ManagementRequestGrantedMsg(msg.getRequest()), getSelf());
			} else if (msg.getRequest().getType() == ManagementRequestType.DISABLE) {
				if (localResources.get(msg.getRequest().getResourceName()).getStatus() == ResourceStatus.ENABLED) {
					// Checking if the user already holds access rights to the resource
					for (UserAccessTuple tup : userAccess.get(msg.getRequest().getResourceName())) {
						if (tup.getUser().equals(msg.getReplyTo())) {
							msg.getReplyTo().tell(new ManagementRequestDeniedMsg(msg.getRequest(), ManagementRequestDenialReason.ACCESS_HELD_BY_USER), getSelf());
							log(LogMsg.makeManagementRequestDeniedLogMsg(msg.getReplyTo(), getSelf(), msg.getRequest(), ManagementRequestDenialReason.ACCESS_HELD_BY_USER));
							return;
						}
					}
					
					if(userAccess.get(msg.getRequest().getResourceName()).size() == 0) {
						localResources.get(msg.getRequest().getResourceName()).disable();
						log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), msg.getRequest().getResourceName(), ResourceStatus.DISABLED));
						log(LogMsg.makeManagementRequestGrantedLogMsg(msg.getReplyTo(), getSelf(), msg.getRequest()));
						msg.getReplyTo().tell(new ManagementRequestGrantedMsg(msg.getRequest()), getSelf());
					} else { // CHECK
						List<ManagementRequestMsg> currDisableReqs = disableRequests.get(msg.getRequest().getResourceName());
						currDisableReqs.add(msg);
						disableRequests.put(msg.getRequest().getResourceName(), currDisableReqs);
					} 
					
					Iterator<AccessRequestMsg> iterator = accessRequestQueue.iterator();
					while (iterator.hasNext()){
						AccessRequestMsg accessMsg = iterator.next();
						if (accessMsg.getAccessRequest().getResourceName().equals(msg.getRequest().getResourceName())) {
							accessMsg.getReplyTo().tell(new AccessRequestDeniedMsg(accessMsg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
							log(LogMsg.makeAccessRequestDeniedLogMsg(accessMsg.getReplyTo(), getSelf(), accessMsg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
							iterator.remove();
						}
					}
				} else if (localResources.get(msg.getRequest().getResourceName()).getStatus() == ResourceStatus.DISABLED) {
					log(LogMsg.makeManagementRequestGrantedLogMsg(msg.getReplyTo(), getSelf(), msg.getRequest()));
					msg.getReplyTo().tell(new ManagementRequestGrantedMsg(msg.getRequest()), getSelf());
				}
			}
			
		}
	}
	
	// ---------------------- Locating Resource Handlers -------------------------------------
	
	public void whoHasResourceRequest(WhoHasResourceRequestMsg msg) throws Exception {
		boolean containsResource = localResources.containsKey(msg.getResourceName());
		getSender().tell(new WhoHasResourceResponseMsg(msg.getResourceName(), containsResource, getSelf()), getSelf());
	}
	
	public void whoHasResourceResponse(WhoHasResourceResponseMsg msg) throws Exception {
		if (msg.getResult() == true) { // found remote manager with required resource
			if (unknownResources.containsKey(msg.getResourceName())) {
				log(LogMsg.makeRemoteResourceDiscoveredLogMsg(getSelf(), msg.getSender(), msg.getResourceName()));
				knownManagers.put(msg.getResourceName(), msg.getSender());
				for(HashMap<Object, Integer> map : unknownResources.get(msg.getResourceName())) {
					for (Object obj : map.keySet()) {
						if (obj instanceof AccessRequestMsg) {
							AccessRequestMsg m = (AccessRequestMsg) obj;
							msg.getSender().tell(m, m.getReplyTo());
						} else if (obj instanceof AccessReleaseMsg) {
							AccessReleaseMsg m = (AccessReleaseMsg) obj;
							msg.getSender().tell(m, m.getSender());
						} else if (obj instanceof ManagementRequestMsg) {
							ManagementRequestMsg m = (ManagementRequestMsg) obj;
							msg.getSender().tell(m, m.getReplyTo());						
						}
					}
				}
				unknownResources.remove(msg.getResourceName());
			}
		} else {
			if (unknownResources.containsKey(msg.getResourceName()) && unknownResources.get(msg.getResourceName()).size() != 0) {
				for (HashMap<Object, Integer> outerMap : unknownResources.get(msg.getResourceName())) {
					for (Object msgKey : outerMap.keySet()) {
						outerMap.compute(msgKey, (key, val) -> (val == null) ? null : val - 1);

						if (outerMap.get(msgKey) == 0) { // base case: no one has resource
							for (HashMap<Object, Integer> map : unknownResources.get(msg.getResourceName())) {
								for (Object o : map.keySet()) {
									if (o instanceof AccessRequestMsg) {
										AccessRequestMsg m = (AccessRequestMsg) o;
										m.getReplyTo().tell(new AccessRequestDeniedMsg(m,
												AccessRequestDenialReason.RESOURCE_NOT_FOUND), getSelf());
										log(LogMsg.makeAccessRequestDeniedLogMsg(m.getReplyTo(), getSelf(),
												m.getAccessRequest(), AccessRequestDenialReason.RESOURCE_NOT_FOUND));
									} else if (o instanceof AccessReleaseMsg) {
										AccessReleaseMsg m = (AccessReleaseMsg) o;
										log(LogMsg.makeAccessReleaseIgnoredLogMsg(m.getSender(), getSelf(),
												m.getAccessRelease()));
									} else if (o instanceof ManagementRequestMsg) {
										ManagementRequestMsg m = (ManagementRequestMsg) o;
										m.getReplyTo().tell(new ManagementRequestDeniedMsg(m,
												ManagementRequestDenialReason.RESOURCE_NOT_FOUND), getSelf());
										log(LogMsg.makeManagementRequestDeniedLogMsg(m.getReplyTo(), getSelf(),
												m.getRequest(), ManagementRequestDenialReason.RESOURCE_NOT_FOUND));
									}
								}
							}
							unknownResources.remove(msg.getResourceName());
						}
					}
				}
			}
		}
	}
	
	// ---------------------- Private Methods ----------------------------------
	
	private void processQueueAndGrantAccess() throws Exception{
		for(AccessRequestMsg accessMsg : accessRequestQueue) {
			boolean accessPossible = true;
			for(UserAccessTuple tup : userAccess.get(accessMsg.getAccessRequest().getResourceName())) {
				// If user already has a write request, he can get another
				if (tup.getAccessType() == AccessType.EXCLUSIVE_WRITE 
					&& !tup.getUser().equals(accessMsg.getReplyTo())) {
					accessPossible = false;
					break;
				} else if (tup.getAccessType() == AccessType.CONCURRENT_READ
					&& !tup.getUser().equals(accessMsg.getReplyTo())
					&& (accessMsg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING
						|| accessMsg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING)) {
					accessPossible = false;
					break;
				}
			}
			
			if (accessPossible) {
				accessRequest(accessMsg);
			}
		}
	}
	
	private void searchForResourceRequest(AccessRequestMsg msg) {
		if (!userAccess.containsKey(msg.getAccessRequest().getResourceName())) {
			userAccess.put(msg.getAccessRequest().getResourceName(), new LinkedList<>());
		}
		
		if (!disableRequests.containsKey(msg.getAccessRequest().getResourceName())) {
			disableRequests.put(msg.getAccessRequest().getResourceName(), new LinkedList<ManagementRequestMsg>());
		}
		
		HashMap<Object, Integer> unknown = new HashMap<>();
		unknown.put(msg, 0);
		
		if (!unknownResources.containsKey(msg.getAccessRequest().getResourceName())) {
			for (ActorRef manager : remoteManagers) {
				unknown.compute(msg, (key, val)-> (val == null) ? null : val + 1);
				manager.tell(new WhoHasResourceRequestMsg(msg.getAccessRequest().getResourceName()), getSelf());
			}
			List<HashMap<Object, Integer>> requestsSent = new LinkedList<>();
			requestsSent.add(unknown);
			unknownResources.put(msg.getAccessRequest().getResourceName(), requestsSent);
		} else {
			unknownResources.get(msg.getAccessRequest().getResourceName()).add(unknown);
		}
		
	}
	
	private void searchForResourceRelease(AccessReleaseMsg msg) {
		if (!userAccess.containsKey(msg.getAccessRelease().getResourceName())) {
			userAccess.put(msg.getAccessRelease().getResourceName(), new LinkedList<>());
		}
		
		if (!disableRequests.containsKey(msg.getAccessRelease().getResourceName())) {
			disableRequests.put(msg.getAccessRelease().getResourceName(), new LinkedList<ManagementRequestMsg>());
		}
		
		HashMap<Object, Integer> unknown = new HashMap<>();
		unknown.put(msg, 0);
		
		if (!unknownResources.containsKey(msg.getAccessRelease().getResourceName())) {
			for (ActorRef manager : remoteManagers) {
				unknown.compute(msg, (key, val)-> (val == null) ? null : val + 1);
				manager.tell(new WhoHasResourceRequestMsg(msg.getAccessRelease().getResourceName()), getSelf());
			}
			List<HashMap<Object, Integer>> requestsSent = new LinkedList<>();
			requestsSent.add(unknown);
			unknownResources.put(msg.getAccessRelease().getResourceName(), requestsSent);
		} else {
			unknownResources.get(msg.getAccessRelease().getResourceName()).add(unknown);
		}
		
	}
	
	private void searchForManagerRequest(ManagementRequestMsg msg) {
		if (!userAccess.containsKey(msg.getRequest().getResourceName())) {
			userAccess.put(msg.getRequest().getResourceName(), new LinkedList<>());
		}
		
		if (!disableRequests.containsKey(msg.getRequest().getResourceName())) {
			disableRequests.put(msg.getRequest().getResourceName(), new LinkedList<ManagementRequestMsg>());
		}
		
		HashMap<Object, Integer> unknown = new HashMap<>();
		unknown.put(msg, 0);
		
		if (!unknownResources.containsKey(msg.getRequest().getResourceName())) {
			for (ActorRef manager : remoteManagers) {
				unknown.compute(msg, (key, val)-> (val == null) ? null : val + 1);
				manager.tell(new WhoHasResourceRequestMsg(msg.getRequest().getResourceName()), getSelf());
			}
			List<HashMap<Object, Integer>> requestsSent = new LinkedList<>();
			requestsSent.add(unknown);
			unknownResources.put(msg.getRequest().getResourceName(), requestsSent);
		} else {
			unknownResources.get(msg.getRequest().getResourceName()).add(unknown);
		}
		
	}
	
	// ------------------------ Private Class(es) -----------------------------------
	
	private class UserAccessTuple {
		private ActorRef user; 
		private AccessType accessType;
		
		public UserAccessTuple(ActorRef u, AccessType a) {
			this.user = u;
			this.accessType  = a;
		}
		
		public ActorRef getUser() {
			return this.user;
		}
		
		public AccessType getAccessType() {
			return this.accessType;
		}
	}
}