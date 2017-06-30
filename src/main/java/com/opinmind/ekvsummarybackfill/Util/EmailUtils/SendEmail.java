package com.opinmind.ekvsummarybackfill.Util.EmailUtils;

import org.apache.log4j.Logger;

import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Created by yzhao on 6/29/17.
 */
public class SendEmail {
    public static void main(String[] args){
        SendEmail sendEmail = new SendEmail();
        sendEmail.send("test", null, "12", "13", false);
    }

    private static Logger log = Logger.getLogger("SendEmail.class");
    private final String from = "backfillticket@gmail.com";

    // Assuming you are sending email from localhost
    private final String host = "smtp.gmail.com";

    // Username
    private final String user = "backfillticket@gmail.com";
    private final String password = "backfill123";

    public void send(String table, String partition, String curYear, String curYearMonth, boolean isFinishedAll){
        // Get system properties
        Properties properties = System.getProperties();

        properties.setProperty("mail.host", host);
        properties.setProperty("mail.smtp.port", "587");
        properties.setProperty("mail.smtp.auth", "true");
        properties.setProperty("mail.smtp.starttls.enable", "true");
        Authenticator auth = new SMTPAuthenticator(user, password);

        // Get the default Session object.
        Session session = Session.getInstance(properties, auth);

        // Set response content type
        MimeMessage message = null;
        try {
            // Create a default MimeMessage object.
            message = new MimeMessage(session);
            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));

            // Set To: header field of the header.
            Address[]  recipients = {new InternetAddress("yi.zhao@adara.com")};
            message.addRecipients(Message.RecipientType.TO, recipients);

            // Set Subject: header field
            if(!isFinishedAll) {
                message.setSubject("Finished backfilling for table:" + table + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth);
                // Now set the actual message
                message.setText("Finished backfilling for table:" + table + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth);
            }else{
                message.setSubject("Finished all backfilling for table:" + table);
                message.setText("Finished all backfilling for table:" + table);
            }
            // Send message
            Transport.send(message);
        } catch (AddressException e) {
            log.error("[SendEmail.send]: ", e);
        } catch (javax.mail.MessagingException e) {
            log.error("[SendEmail.send]: ", e);
        }
    }


    private class SMTPAuthenticator extends Authenticator {
        private PasswordAuthentication authentication;

        public SMTPAuthenticator(String login, String password) {
            authentication = new PasswordAuthentication(login, password);
        }

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            return authentication;
        }
    }
}
