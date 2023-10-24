    package com.apag.p2plus.vrspurchase;

/**
 * Package: com.apag.p2plus.vrspurchase Class: VrsBestellimportCoupa.java
 * --------------------------------------------------------------- Copyright
 * VRS, 2022 ---------------------------------------------------------------
 * State: Design 19.01.2022 pepperling generated
 * ---------------------------------------------------------------
 */
import com.apag.p2plus.p2core.Clients;
import com.apag.p2plus.p2core.Job;
import com.apag.p2plus.p2core.P2Time;
import com.apag.p2plus.p2core.Transaction;
import com.apag.p2plus.p2core.p2exceptions.P2Exception;
import com.apag.p2plus.p2core.p2exceptions.P2ProblemException;
import com.apag.p2plus.p2masterdata.MandantFactory;
import com.apag.p2plus.p2masterdata.MandantObject;
import com.apag.p2plus.p2objects.BusinessObject;
import com.apag.p2plus.p2objects.ObjectCollection;
import com.apag.p2plus.p2objects.ObjectConstants;
import static com.apag.p2plus.p2objects.ObjectConstants.READ_LOCK;
import com.apag.p2plus.p2objects.ObjectManager;
import com.apag.p2plus.p2purchase.BestellungFactory;
import com.apag.p2plus.p2purchase.BestellungObject;
import com.apag.p2plus.p2purchase.BestellungPosFactory;
import com.apag.p2plus.p2purchase.BestellungPosObject;
import com.apag.p2plus.vrscore.MsgContext;
import com.apag.p2plus.vrstools.VrsErrorHelper;
import java.util.List;

/**
 *
 * @author pepperling
 */
public class VrsBestellImportCoupaUtil {

  /**
   * Erstellt eine Bestellung aus Coupa Daten
   *
   * @param jobId
   *
   * @throws P2Exception
   */
  public void createBestellungFromCoupa(String jobId) throws P2Exception {
    String originalMandant = Clients.getCurrentClient();
    boolean success = false;
    ObjectManager head_manager = new ObjectManager();
    VrsErrorHelper errorHelper = new VrsErrorHelper(new MsgContext(), new VrsCoupaInterfaceLogger());

    try {
      Job.jobStart(jobId);
      Job.jobSetInfo(jobId, "Lese Headerdaten aus Datenbank");
      String sql = " vrsimportstatus = 1 order by coupaponumber, internalrevision";
      ObjectCollection<VrsCoupaPoHeaderObject> coupaPOHeaderOC = new ObjectCollection<>(head_manager, VrsCoupaPoHeaderObject.TABLE, sql, ObjectConstants.READ_LOCK);
      coupaPOHeaderOC.readAll();
      List<VrsCoupaPoHeaderObject> coupaPoHeadList = coupaPOHeaderOC.getList();
      head_manager.release();
      Transaction.complete();
      int i = 1;
      for (VrsCoupaPoHeaderObject coupaHeaderObj : coupaPoHeadList) {
        Job.jobProgress(jobId, i++, coupaPoHeadList.size());

        try {
          success = false;
          ObjectManager manager = new ObjectManager();

          String fileName = coupaHeaderObj.getVrsfilename();
          String posSql = "VRSIMPORTSTATUS = 1 and orderheaderid = " + coupaHeaderObj.getCoupapoid() + " and VRSFILENAME = '" + fileName + "' order by orderheaderid, linenumber, updatedat";
          ObjectCollection<VrsCoupaPoLineObject> coupaPOLineOC = new ObjectCollection<>(manager, VrsCoupaPoLineObject.TABLE, posSql, ObjectConstants.READ_LOCK);
          createBestellung(manager, coupaHeaderObj, coupaPOLineOC, jobId);
          success = true;
        } catch (Exception e) {
          Transaction.abort();
          errorHelper.addLog("Coupa Bestellung: " + coupaHeaderObj.getCoupaponumber() + " " + this.getClass().getName() + ":" + e.getStackTrace()[0].getMethodName(), e);
          Transaction.spawn();
          ObjectManager manager = new ObjectManager();
          errorHelper.logError();
          Integer ptID = errorHelper.getLogIDs().get(0);
          errorHelper.clear();
          coupaHeaderObj.attach();
          coupaHeaderObj.setVrsimportstatus(VrsCoupaPoHeaderObject.COUPAPOHEADER_STATUS_FEHLER);
          coupaHeaderObj.setPtId(String.valueOf(ptID));
          coupaHeaderObj.update();
          Transaction.complete();
        } finally {
          if (success) {
            Transaction.complete();
          }
        }
      }

    } catch (P2Exception e) {
      errorHelper.addLog(this.getClass().getName() + ":" + e.getStackTrace()[0].getMethodName(), e);
    } finally {
      Clients.setCurrentClient(originalMandant);
      //manager.release();
      errorHelper.logError();
      errorHelper.clear();
      //Transaction.complete();
    }
  }

  /**
   * Aktualisiert die Bestellposition bei Wareneingangsmeldung aus Coupa
   *
   * @param jobId
   *
   * @throws P2Exception
   */
  public void updateBestellungWareneingang(String jobId) throws P2Exception {
    Job.jobStart(jobId);
    Job.jobSetInfo(jobId, "Aktualisiere Bestellung");
    ObjectManager manager = new ObjectManager();
    String originalMandant = Clients.getCurrentClient();
    String sql = " vrsimportstatus = 1 ";
    boolean success = false;
    ObjectCollection<VrsCoupaInventoryObject> objColWe = new ObjectCollection<>(manager, VrsCoupaInventoryObject.TABLE, sql, ObjectConstants.READ_LOCK);
    objColWe.readAll();
    List<VrsCoupaInventoryObject> weList = objColWe.getList();
    VrsErrorHelper errorHelper = new VrsErrorHelper(new MsgContext(), new VrsCoupaInterfaceLogger());
    int i = 1;
    for (VrsCoupaInventoryObject coupaWe : weList) {
      Transaction.spawn();
      Job.jobProgress(jobId, i++, weList.size());
      try {
        if (String.valueOf(coupaWe.getCoupaponumber()).length() < 3) {
          throw new P2ProblemException("Ungültige Coupa-Bestelnummer: " + coupaWe.getCoupaponumber());
        }
        String client = getClientFromPoHeader(String.valueOf(coupaWe.getCoupaponumber()), manager);
        Clients.setCurrentClient(client);
        success = false;
        sql = " VRSCOUPAPONUMBER = '" + coupaWe.getCoupaponumber() + "'";
        List<BusinessObject> bestList = new ObjectCollection<>(manager, BestellungObject.TABLE, sql, ObjectConstants.READ_LOCK).getList();
        if (bestList.isEmpty()) {
          throw new P2ProblemException("Unbekannte Coupa-Bestellung: " + coupaWe.getCoupaponumber());
        }
        BestellungObject bestellung = (BestellungObject) bestList.get(0);
        sql = " BESTELLUNG = '" + bestellung.getBestellung() + "' AND POSITION = " + coupaWe.getOrderlinenumber();
        
        Transaction.spawn();
        coupaWe.attach();
        coupaWe.setBestellung(bestellung.getBestellung());
        coupaWe.update();
        Transaction.complete();
        
        List<BusinessObject> bestPosList = new ObjectCollection<>(manager, BestellungPosObject.TABLE, sql, ObjectConstants.READ_LOCK).getList();
        if (bestPosList.isEmpty()) {
          throw new P2ProblemException("Unbekannte Coupa-Bestellposition: " + coupaWe.getCoupaponumber() + " Position: " + coupaWe.getOrderlinenumber());
        }
        BestellungPosObject posObj = (BestellungPosObject) bestPosList.get(0);
        posObj.attach();
        posObj.setGeliefertemenge((posObj.getGeliefertemenge() + coupaWe.getQuantity()));
        if((posObj.getMenge() == posObj.getGeliefertemenge() || coupaWe.getOrderlinestatus().equalsIgnoreCase("received")) && posObj.getStatus() < 7){
          posObj.setStatus(BestellungPosObject.STATUS_BESTAETIGT);
          posObj.update();
          posObj.setStatus(BestellungPosObject.STATUS_GELIEFERT);
        }else{
          //Aufgrund von Updates auf Bestellungen kann dieser Status hier nicht gesetzt werden.
          //posObj.setStatus(BestellungPosObject.STATUS_INTRANSIT);
        }
        posObj.update();
        coupaWe.attach();
        coupaWe.setVrsimportstatus(VrsCoupaPoHeaderObject.COUPAPOHEADER_STATUS_VERARBEITET);
        coupaWe.update();
        success = true;
      } catch (Exception e) {
        Transaction.abort();
        errorHelper.addLog(this.getClass().getName() + ":" + e.getStackTrace()[0].getMethodName(), e);
        Transaction.spawn();
        errorHelper.logError();
        Integer ptID = errorHelper.getLogIDs().get(0);
        errorHelper.clear();
        coupaWe.attach();
        coupaWe.setVrsimportstatus(VrsCoupaPoHeaderObject.COUPAPOHEADER_STATUS_FEHLER);
        coupaWe.setPtId(String.valueOf(ptID));
        coupaWe.update();
        Transaction.complete();
      } finally {
        if (success) {
          Transaction.complete();
        }
        errorHelper.logError();
        errorHelper.clear();
        Clients.setCurrentClient(originalMandant);
      }
    }

  }

  /**
   * Legt eine Bestellung aus Coupa Daten an
   *
   * @param manager
   * @param importHeaderObj
   * @param posList
   *
   * @throws P2Exception
   */
  private void createBestellung(final ObjectManager manager,
                                VrsCoupaPoHeaderObject importHeaderObj,
                                ObjectCollection<VrsCoupaPoLineObject> posList,
                                String jobId) throws P2Exception {

    String client = getClientFromPoHeader(importHeaderObj.getCoupaponumber(), manager);
    Clients.setCurrentClient(client);

    //Prüfen ob schon eine Bestellung existiert, die aktualisiert werden kann
    String bestNr = importHeaderObj.getApplusbestellung();
    String bestSql = " VRSCOUPAPONUMBER = '" + importHeaderObj.getCoupaponumber() + "'";
    List<BusinessObject> bestList = new ObjectCollection<>(manager, BestellungObject.TABLE, bestSql, ObjectConstants.READ_LOCK).getList();
    BestellungObject bestellungObj = null;
    boolean updateBestellung = false, updateBestellungPos = false;
    short oldStatus = 0;
    if(bestList.size() > 0){
      bestellungObj = (BestellungObject)bestList.get(0);
      if(bestellungObj.getStatus() <  BestellungObject.STATUS_FAKTURIERT){
        oldStatus = bestellungObj.getStatus();
        bestellungObj.setStatus(BestellungObject.STATUS_BEREIT);
        bestellungObj.update();
        updateBestellung = true;
      }else{
         throw new P2ProblemException("Coupa Bestellung: " + importHeaderObj.getCoupaponumber() + "/ Eine Änderung der Bestellung in diesem Status ist nicht mehr möglich");
      }
      if(importHeaderObj.getStatus().equalsIgnoreCase("cancelled") || importHeaderObj.getStatus().equalsIgnoreCase("soft_closed")){
        bestellungObj.setStatus(BestellungObject.STATUS_STORNIERT);
        bestellungObj.update();
        posList.readAll();
        for (final VrsCoupaPoLineObject coupaPOLine : posList) {
          coupaPOLine.attach();
          coupaPOLine.setVrsimportstatus(VrsCoupaPoLineObject.COUPAPOLINE_STATUS_VERARBEITET);
          coupaPOLine.update();
        }
        return;
      }
    }else{
      bestellungObj = BestellungFactory.create(manager);      
    }        
    try {
      if (importHeaderObj.getSuppliernumber() == null) {
        throw new P2ProblemException("Coupa Bestellung: " + importHeaderObj.getCoupaponumber() + "/ Es wurde keine Lieferantennummer übergeben.");
      }
      posList.readAll();
      if(posList.getList().size() > 0){
        bestellungObj.setBedarfstraeger(getApplusBedarfstraeger(posList.getList().get(0), manager));
      }
      bestellungObj.setUr_standort(getApplusStandort( bestellungObj.getBedarfstraeger(), manager));
      String supplierNumber = Character.toLowerCase(importHeaderObj.getSuppliernumber().charAt(0)) == 'k' ? importHeaderObj.getSuppliernumber() + ".001" : String.format("K%s.001", importHeaderObj.getSuppliernumber());
      bestellungObj.setLort(importHeaderObj.getShiptoaddresscity());
      bestellungObj.setLplz(importHeaderObj.getShiptoaddresspostalcode());
      bestellungObj.setLstrasse(importHeaderObj.getShiptoaddressstreet1());
      bestellungObj.setLname(importHeaderObj.getShiptouserfirstname() + " " + importHeaderObj.getShiptouserlastname());
      bestellungObj.setLadrmanuell((short)1);
      if(!updateBestellung){
        bestellungObj.setAdresse(supplierNumber);
      }
      bestellungObj.setWaehrung(importHeaderObj.getCurrency());
      bestellungObj.setVrscoupaponumber(importHeaderObj.getCoupaponumber());
      bestellungObj.update();
    } catch (Exception ex) {
      throw new P2ProblemException("Coupa Bestellung: " + importHeaderObj.getCoupaponumber() + " Fehler: " + ex.getMessage());
    }
    Job.jobSetInfo(jobId, "Für die Coupa Bestellung " + importHeaderObj.getCoupaponumber() + " wurde APplus Bestellung " + bestellungObj.getBestellung() + " angelegt.");
    P2Time spaetesteLieferBisDate = new P2Time("01.01.1950");
    short posNr = 0;
    Job.jobSetInfo(jobId, "Lege Positionen an");
    for (final VrsCoupaPoLineObject coupaPOLine : posList) {
      try {
        updateBestellungPos = false;
        posNr = Short.valueOf(coupaPOLine.getLinenumber());
        String sql = " BESTELLUNG = '" + bestellungObj.getBestellung() + "' AND POSITION = " + posNr;
        List<BusinessObject> bestPosList = new ObjectCollection<>(manager, BestellungPosObject.TABLE, sql, ObjectConstants.READ_LOCK).getList();
        BestellungPosObject bestellungPosObj;
        short oldPosStatus = 0;
        if (bestPosList.size() > 0) {
          bestellungPosObj = (BestellungPosObject) bestPosList.get(0);
          oldPosStatus = bestellungPosObj.getStatus();
          if(bestellungObj.getStatus() <  BestellungObject.STATUS_FAKTURIERT){
            bestellungPosObj.setStatus(BestellungPosObject.STATUS_BEREIT);
            bestellungPosObj.update();
            updateBestellungPos = true; 
            if(coupaPOLine.getStatus().equalsIgnoreCase("cancelled") || coupaPOLine.getStatus().toLowerCase().contains("soft_closed".toLowerCase())){
              bestellungPosObj.setStatus(BestellungPosObject.STATUS_STORNIERT);
              bestellungPosObj.update();
              coupaPOLine.attach();
              coupaPOLine.setVrsimportstatus(VrsCoupaPoLineObject.COUPAPOLINE_STATUS_VERARBEITET);
              coupaPOLine.setApplusbestellung(bestNr);
              coupaPOLine.update();
              continue;
            }           
          }else{
            coupaPOLine.attach();
            coupaPOLine.setVrsimportstatus(VrsCoupaPoLineObject.COUPAPOLINE_STATUS_VERARBEITET);
            coupaPOLine.setApplusbestellung(bestNr);
            coupaPOLine.update();
            continue;            
          }
        } else {
          bestellungPosObj = BestellungPosFactory.create(manager, bestellungObj.getBestellung(), posNr);
        }
        if (coupaPOLine.getNeedbydate().isLaterThan(spaetesteLieferBisDate)) {
          spaetesteLieferBisDate = coupaPOLine.getNeedbydate();
        }
        //bestellungPosObj.setArtikel(coupaPOLine.getArtikel());
        bestellungPosObj.setName(coupaPOLine.getDescription().length() > 63 ? coupaPOLine.getDescription().substring(0, 62) : coupaPOLine.getDescription());
        bestellungPosObj.setLName(coupaPOLine.getDescription().length() > 63 ? coupaPOLine.getDescription().substring(0, 62) : coupaPOLine.getDescription());

        bestellungPosObj.setVbme(getApplusMe(coupaPOLine, manager));
        bestellungPosObj.setEkme(getApplusMe(coupaPOLine, manager));
        bestellungPosObj.setMenge(coupaPOLine.getQuantity() == 0 ? 1 : coupaPOLine.getQuantity());
        bestellungPosObj.setEkmenge(coupaPOLine.getQuantity()  == 0 ? 1 : coupaPOLine.getQuantity());
        bestellungPosObj.setEkpackmenge(1.0);

        bestellungPosObj.setNetto(coupaPOLine.getPrice());

//      // Kontierung
        String accountingType = coupaPOLine.getSegment2();
        if (accountingType != null) {
          if (accountingType.equals("CO")) {
            bestellungPosObj.setKstr(coupaPOLine.getSegment3());
          } else if (accountingType.equals("CC")) {
            bestellungPosObj.setKostenstelle(coupaPOLine.getSegment3());
          } else if (accountingType.equals("INV")) {
            bestellungPosObj.setProjekt(coupaPOLine.getSegment3());
          } else if (accountingType.equals("RIO")) {
            bestellungPosObj.setInnenauftrag(coupaPOLine.getSegment3());
          } else if (accountingType.equals("RPSP")) {
            bestellungPosObj.setString("PSP", coupaPOLine.getSegment3());
          } else {
            throw new P2ProblemException("Unbekannte Kontierungsart: " + accountingType + " für PO: " + importHeaderObj.getCoupaponumber());
          }
        } else {
          throw new P2ProblemException("Unbekannte Kontierungsart: " + accountingType + " für PO: " + importHeaderObj.getCoupaponumber());
        }
        
        if(!accountingType.equals("RPSP")){
          bestellungPosObj.setPsp(coupaPOLine.getSegment4());
        }

        bestellungPosObj.setAufwandskonto(coupaPOLine.getSegment1());

        bestellungPosObj.setBedarfstermin(coupaPOLine.getNeedbydate());
        if (!updateBestellungPos) {
          bestellungPosObj.setPosition(posNr);
          String posOrder = "///" + String.valueOf(posNr);
          posOrder = posOrder.substring(posOrder.length() - 4);
          bestellungPosObj.setUserposAndOrder(String.valueOf(posNr), posOrder);
        }

        bestellungPosObj.setText(coupaPOLine.getDescription());
        if (coupaPOLine.getMandatecustomerorderno() != null) {
          bestellungPosObj.setVerursacherart("V");
          bestellungPosObj.setVerursacher(coupaPOLine.getMandatecustomerorderno());
        }
        bestellungPosObj.update();
        if(oldPosStatus != 0){
          bestellungPosObj.setStatus(oldPosStatus);
          bestellungPosObj.update();
        }
        coupaPOLine.attach();
        coupaPOLine.setVrsimportstatus(VrsCoupaPoLineObject.COUPAPOLINE_STATUS_VERARBEITET);
        coupaPOLine.setApplusbestellung(bestNr);
        coupaPOLine.update();
      } catch (Exception ex) {        
        Transaction.spawn();
        coupaPOLine.attach();
        coupaPOLine.setVrsimportstatus(VrsCoupaPoLineObject.COUPAPOLINE_STATUS_FEHLER);
        coupaPOLine.update();
        Transaction.complete();
        throw new P2ProblemException(ex.getMessage());
      }
    } // for pos
    importHeaderObj.attach();
    importHeaderObj.setApplusbestellung(bestellungObj.getBestellung());
    importHeaderObj.setVrsimportstatus(VrsCoupaPoHeaderObject.COUPAPOHEADER_STATUS_VERARBEITET);
    importHeaderObj.update();
    if(spaetesteLieferBisDate.isLaterThan(new P2Time("01.01.2000"))){
      bestellungObj.setZuliefernbis(spaetesteLieferBisDate);
    }
    bestellungObj.calcHeaderPrices();
    if(oldStatus != 0){
      bestellungObj.setStatus(oldStatus);
    }else{
    bestellungObj.setStatus(BestellungObject.STATUS_VERSENDET);
    }
    bestellungObj.update();
  }

  /**
   *
   */
  public static void releaseBestellung() {

  }

  /**
   *
   */
  public static void deliverdBestellung() {

  }

  /**
   * Konvertiert die Mengeheit von Coupa in das APplus Format
   *
   * @param coupaPOLine
   *
   * @return
   */
  private String getApplusMe(VrsCoupaPoLineObject coupaPOLine,
                             ObjectManager manager) throws P2Exception {
    if(coupaPOLine.getUom() != null && !coupaPOLine.getUom().isEmpty()){
      String coupaMe = coupaPOLine.getUom();
      VrsCoupaMappingObject mappingObj = VrsCoupaMappingFactory.searchForMe(manager, coupaMe, READ_LOCK, false);
      if (mappingObj == null) {
        throw new P2ProblemException("Umbekannte Mengeneinheit: " + coupaPOLine.getUom());
      }
      return mappingObj.getApplus();
    }
    return "Stück";
  }
  
  /**
   * Konvertiert des Coupa Standorts in Bedarfträger
   *
   * @param coupaPOLine
   *
   * @return
   */
  private String getApplusBedarfstraeger(VrsCoupaPoLineObject coupaPOLine,
                             ObjectManager manager) throws P2Exception {
    String coupaStandort = coupaPOLine.getSegment5();
    VrsCoupaMappingObject mappingObj = VrsCoupaMappingFactory.searchForBedarfstraeger(manager, coupaStandort, READ_LOCK, false);
    return mappingObj == null ? "" : mappingObj.getApplus();
  }

   /**
   * Konvertiert Coupa Bedarfsträger in APplus Standort
   *
   * @param applussAbteilung zuvor ermittelte Abteilung/Bedarfsträger
   *
   * @return
   */
  private String getApplusStandort(String applussAbteilung, ObjectManager manager) throws P2Exception {
    String sql = "SELECT STANDORT FROM VRSBEDARFSTRAEGER " + 
              "WHERE ABTEILUNG = '" + applussAbteilung + "' AND MANDANT = '" + Clients.getCurrentClient() + "'";
    boolean managerIgnoreClients = manager.isIgnoreClients();
    manager.setIgnoreClients(true);
    String standortFromBedarfstraeger = manager.getScalarValue("VRSBEDARFSTRAEGER", sql );
    manager.setIgnoreClients(managerIgnoreClients);
    return applussAbteilung.isEmpty() ? "Default" : standortFromBedarfstraeger;
  }
  
  /**
   * Ermittel den Mandanen aus der Bestellnummer
   *
   * @param importHeaderObj
   * @param manager
   *
   * @return
   *
   * @throws P2Exception
   */
  private String getClientFromPoHeader(String orderNumber,
                                       ObjectManager manager) throws P2Exception {
    String coupa_buchungskreis = orderNumber.substring(0, 3);
    String mandant = "0" + coupa_buchungskreis;

    String sql = "select mandantid from mandant where fibuid = '" + mandant + "'";
    String mandantid = manager.getScalarValue(MandantObject.TABLE, sql);

    MandantObject mandObj = MandantFactory.search(manager, mandantid, READ_LOCK, true);

    return mandObj.getMandantid();
  }
}
