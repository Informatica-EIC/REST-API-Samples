package com.infa.eic.sample;

import difflib.Chunk;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.common.collect.ObjectArrays;
import com.opencsv.CSVWriter;

public class StructureDiff {

	private final File original;
	private final File revised;
	public static final String version = "1.1";

	String mail_smtp_host;
	String mail_smtp_port;
	String mail_user;
	String mail_pwd;
	List<String> recipients = new ArrayList<String>(); // ({});
	String mail_smtp_auth;
	String mail_smtp_socketFactory_port;
	String mail_smtp_socketFactory_class;
	String mail_format;

	String leftName;
	String rightName;
	String resourceName;

	boolean includeAxonLinks = false;

	/**
	 * counters for changes
	 */
	int totalInserts = 0;
	int totalUpdates = 0;
	int totalDeletes = 0;

	/**
	 * constructor, initialize the - read the mail property settings
	 * 
	 * @param original
	 * @param revised
	 * @param propertyFile
	 */
	public StructureDiff(File original, File revised, String propertyFile) {
		this.original = original;
		this.revised = revised;

		System.out.println(this.getClass().getSimpleName() + " " + version + " diffing..." + original + " to " + revised
				+ " pros=" + propertyFile);

		this.setupCSVFile(original.getParentFile().getAbsolutePath(), original.getName(), revised.getName());
		leftName = original.getName();
		rightName = revised.getName();
		if (rightName.contains("__")) {
			// get the resource name
			resourceName = rightName.substring(0, rightName.indexOf("__"));
		}

		// read the email settings from propertyFile...
		try {
			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop;
			prop = new Properties();
			prop.load(fileInput);
			fileInput.close();
			mail_smtp_host = prop.getProperty("mail.smtp.host");
			mail_smtp_port = prop.getProperty("mail.smtp.port");
			mail_user = prop.getProperty("mail.user");
			mail_pwd = prop.getProperty("mail.pwd");
			String mailRecipients = prop.getProperty("mail.recipients");
			if (mailRecipients != null && mailRecipients.length() > 0) {
				recipients = new ArrayList<String>(Arrays.asList(prop.getProperty("mail.recipients").split(",")));
			}
			mail_smtp_auth = prop.getProperty("mail.smtp.auth", "false");
			mail_smtp_socketFactory_port = prop.getProperty("mail.smtp.socketFactory.port");
			mail_smtp_socketFactory_class = prop.getProperty("mail.smtp.socketFactory.class");
			mail_format = prop.getProperty("mail.format", "text/plain");

			System.out.println("mail settings: " + mail_smtp_host + ":" + mail_smtp_port + " mail.user:" + mail_user
					+ " mail.pwd:" + mail_pwd.replaceAll(".", "*") + " format:" + mail_format + " mail.recipients:"
					+ recipients);
			// System.out.println("recipients isEmpty()? " + recipients.isEmpty() + " " +
			// recipients.size());
			if (mail_smtp_host.isEmpty() || recipients.isEmpty()) {
				System.out.println("\temail compare results will not be sent:  missing mail.smtp.host setting");
			}

			// also get the setting for whether Axon objects should be included
			// @todo - figure out a better way to do this - like exporting the column
			// headers in the report
			includeAxonLinks = Boolean.parseBoolean(prop.getProperty("includeAxonTermLink", "false"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	} // end constructor

	public List<Chunk> getChangesFromOriginal() throws IOException {
		return getChunksByType(Delta.TYPE.CHANGE);
	}

	public List<Chunk> getInsertsFromOriginal() throws IOException {
		return getChunksByType(Delta.TYPE.INSERT);
	}

	public List<Chunk> getDeletesFromOriginal() throws IOException {
		return getChunksByType(Delta.TYPE.DELETE);
	}

	private List<Chunk> getChunksByType(Delta.TYPE type) throws IOException {
		final List<Chunk> listOfChanges = new ArrayList<Chunk>();
		final List<Delta> deltas = getDeltas();
		for (Delta delta : deltas) {
			if (delta.getType() == type) {
				listOfChanges.add(delta.getRevised());
			}
		}
		return listOfChanges;
	}

	/**
	 * get the deltas - comparing the original File to the revised file
	 * 
	 * @return list of changes
	 * @throws IOException
	 */
	private List<Delta> getDeltas() throws IOException {

		final List<String> originalFileLines = fileToLines(original);
		final List<String> revisedFileLines = fileToLines(revised);

		final Patch patch = DiffUtils.diff(originalFileLines, revisedFileLines);

		return patch.getDeltas();
	}

	/**
	 * read all lines from a file into a list of Strings
	 * 
	 * @param file the file to read
	 * @return
	 * @throws IOException
	 */
	private List<String> fileToLines(File file) throws IOException {
		final List<String> lines = new ArrayList<String>();
		String line;
		final BufferedReader in = new BufferedReader(new FileReader(file));
		while ((line = in.readLine()) != null) {
			lines.add(line);
		}
		in.close();
		return lines;

	}

	public static void main(String[] args) {
		String from = args[0];
		String to = args[1];
		String propertyFile = args[2];

		System.out.println("from file: " + from + " tofile=" + to + " using property file: " + propertyFile);

		File fromFile = new File(from);
		File toFile = new File(to);

		try {
			StructureDiff sd = new StructureDiff(fromFile, toFile, propertyFile);

			sd.processDiffs();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	StringBuffer consoleLog = new StringBuffer();
	StringBuffer emailDetail = new StringBuffer();
	StringBuffer messageHTMLx = new StringBuffer();
	StringBuffer messageTEXT = new StringBuffer();

	/**
	 * look at the changes made - if there are changes, send email with diff report
	 * 
	 * @throws RuntimeException
	 * 
	 */
	protected void processDiffs() throws IOException, RuntimeException {
		StringBuffer message = new StringBuffer();
		// html message format (diffs as a table)
		// StringBuffer messageHTML = new StringBuffer();
		message.append("Comparing database structures: \n\t" + original + " \n\t" + revised + "\n");
		System.out.println("\t" + this.getClass().getSimpleName() + " Comparing database structures:\n\t\t" + original
				+ "\n\t\t" + revised);
		int changes = 0;

		List<Delta> deltas = getDeltas(); // executes the compare

		messageHTMLx.append("<h2>Enterprise Data Catalog Change report:  resource=" + resourceName + "</h2>\n");
		messageHTMLx.append("<h3>Comparing: " + leftName + " to " + rightName + "</h3>\n");
		messageHTMLx.append("<p/>\n");
		if (this.includeAxonLinks) {
			messageHTMLx.append(
					"<table border=\"1\"><tr><td>Change Type</td><td>Database</td><td>Schema</td><td>Table</td><td>Column</td><td>Type</td><td>Length</td><td>Scale</td><td>Axon Term</td><td>Axon Term Id</td></tr>\n");
		} else {
			messageHTMLx.append(
					"<table border=\"1\"><tr><td>Change Type</td><td>Database</td><td>Schema</td><td>Table</td><td>Column</td><td>Type</td><td>Length</td><td>Scale</td></tr>\n");
		}

		// analyze the deltas... (testing - to get better results than a standard diff
		// process)
		// note: this will update messageHTMLx (via writeDiff() methods)
		changes = analyzeChanges(deltas);

		// System.out.println("messageTEXT=");
		// System.out.println(messageTEXT);

		// System.out.println("messageHTML=");
		// System.out.println(messageHTMLx);

		messageHTMLx.append("</table>\n");

		writer.flush();

		// prepare & send the email
		if (changes > 0 && !(mail_smtp_host.isEmpty() || recipients.isEmpty())) {
			/**
			 * mail result
			 */
			// Get properties object
			System.out.println("\tpreparing email message:");
			Properties props = new Properties();
			props.put("mail.smtp.host", mail_smtp_host);
			if (mail_smtp_auth.equalsIgnoreCase("true")) {
				props.put("mail.smtp.socketFactory.port", mail_smtp_socketFactory_port);
				props.put("mail.smtp.socketFactory.class", mail_smtp_socketFactory_class);
			}
			// props.put("mail.smtp.auth", this.mail_smtp_auth);

			// props.put("mail.smtp.socketFactory.port", this.mail_smtp_socketFactory_port);
			// props.put("mail.smtp.socketFactory.class",
			// this.mail_smtp_socketFactory_class);
			props.put("mail.smtp.auth", this.mail_smtp_auth);
			props.put("mail.smtp.port", this.mail_smtp_port);

			// prep/send email if we know the host/port
			// get Session
			Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(mail_user, mail_pwd);
				}
			});
			// compose message
			try {
				MimeMessage mimeMessage = new MimeMessage(session);
				for (String email : recipients) {
					mimeMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(email));
				}
				// mimeMessage.addRecipient(Message.RecipientType.TO,new
				// InternetAddress("dwrigley@informatica.com"));
				mimeMessage.setSubject(
						"db structure change report - resource:" + resourceName + " " + changes + " changes");
				// mimeMessage.setText(message.toString());
				// mimeMessage.setText(messageHTML.toString());

				// creates message part
				MimeBodyPart messageBodyPart = new MimeBodyPart();
				if (mail_format.equals("text/html")) {
					messageBodyPart.setContent(messageHTMLx.toString(), mail_format);
				} else {
					messageBodyPart.setContent(message.toString(), "text/plain");
				}
				// messageBodyPart.setContent(message.toString(), "text/plain");
				// messageBodyPart.setContent(messageHTML.toString(), mail_format);
				// creates multi-part
				Multipart multipart = new MimeMultipart();
				multipart.addBodyPart(messageBodyPart);
				MimeBodyPart attachPart = new MimeBodyPart();

				// try {
				attachPart.attachFile(diffFileName);
				// } catch (IOException ex) {
				// ex.printStackTrace();
				// }

				multipart.addBodyPart(attachPart);
				// sets the multi-part as e-mail's content
				mimeMessage.setContent(multipart);
				System.out.println("sending message");
				Transport.send(mimeMessage);
				System.out.println("message sent successfully");
			} catch (MessagingException e) {
				System.out.println("error sending email " + e.getMessage());
				// throw new RuntimeException(e);
			}

		} else {
			if (changes == 0) {
				System.out.println("\tno changes - no email sent");
			} else {
				System.out.println("\tchanges found - no mail server or recipients configured: host=" + mail_smtp_host
						+ " recipients=" + recipients);
			}
		}

		this.closeCSVFile();
	}

	/**
	 * look at the deltas & figure out if they are real changes
	 * (db/schema/table/column are the same) or a combination of delete & add (look
	 * similar but are not
	 * 
	 * example 1 (2 on the left - deleted)
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_CONSUMERKEY|CLOB|4000|0
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_CUSTOMPROPERTIES|CLOB|4000|0
	 * 
	 * compared to (1 on the right - added)
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_CLIENTSECRET|CLOB|4000|0
	 * 
	 * example 2 (2 on the left - 2nd line matches first line on right, only - other
	 * 3 on right are new)
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_STAGINGDIRECTOR1|VARCHAR2|4000|0
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_STAGINGDIRECTORY|CLOB|4000|0
	 * 
	 * right (first line matches 2nd from left chunk)
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_STAGINGDIRECTORY|VARCHAR2|4000|0
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_STORAGEACCOUNTKEY|CLOB|4000|0
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_SUBNETNAME|VARCHAR2|1536|0
	 * informatica|INFAMODEL1021|PO_SDKCONNECTINFOMODELEXT|POS_SUBSCRIPTIONID|VARCHAR2|3060|0
	 * 
	 * example 3 - 2 on the left (first left matches first right)
	 * 
	 * informatica|INFAMODEL1021|PO_BOXEDRECEIVEROBJEC1|POB_VALU1|NUMBER|1|0
	 * informatica|INFAMODEL1021|PO_BOXEDRECEIVEROBJEC1|POB_VALU2|VARCHAR2|1536|0
	 * 
	 * right informatica|INFAMODEL1021|PO_BOXEDRECEIVEROBJEC1|POB_VALU1|CLOB|4000|0
	 * informatica|INFAMODEL1021|PO_BOXEDRECEIVEROBJEC1|POB_VALU2|NUMBER|1|0
	 * informatica|INFAMODEL1021|PO_BOXEDRECEIVEROBJEC1|POB_VALU3|VARCHAR2|1536|0
	 * 
	 * 2 type/length changes - 1 new field
	 * 
	 * example 4: changes that are really deletes & inserts (different db/sch/tab) -
	 * but diff sees them as changes when it is just a combination if delete/add
	 * 
	 * left informatica|DATA_WAREHOUSE|CUST_DATA_WAREHOUSE|ADDRESS1|VARCHAR2|2000|0
	 * informatica|DATA_WAREHOUSE|CUST_DATA_WAREHOUSE|CITY|VARCHAR2|2000|0
	 * 
	 * right informatica|WAREHOUSE|CUSTOMER_MAIL_LIST|ADDR_LN_1|NVARCHAR2|400|0
	 * informatica|WAREHOUSE|CUSTOMER_MAIL_LIST|CITY|NVARCHAR2|100|0
	 * informatica|WAREHOUSE|CUSTOMER_MAIL_LIST|GREETING|VARCHAR2|60|0
	 * 
	 * 
	 */
	@SuppressWarnings("unchecked")
	private int analyzeChanges(List<Delta> theDeltas) {
		// we can't trust the changes
		/**
		 * we can't trust the changes example: a change may have 2 lines on the from
		 * side and 4 on the to (chunk of changes) so we need to see if the
		 * db/schema/table/column name changed (or just the type) if the
		 * db/schema/table/column name changed - mark it as a delete & insert if the
		 * object id is the same - then it is a real change (data type/length etc
		 * 
		 */

		int dtot = 0;
		int itot = 0;
		int utot = 0;
		Map<String, String> matches = new HashMap<String, String>();

		for (Delta delta : theDeltas) {
			// what is the type (delete and insert) no extra work needed
			if (delta.getType() == Delta.TYPE.INSERT) {
				itot += delta.getRevised().getLines().size();
				for (String row : (List<String>) delta.getRevised().getLines()) {
					// System.out.println(">>>INSERT: " + row);
					writeDiff("INSERT", row);

				}

				// System.out.println(">>>INSERT: " + );
			} else if (delta.getType() == Delta.TYPE.DELETE) {
				dtot += delta.getOriginal().getLines().size();
				for (String row : (List<String>) delta.getOriginal().getLines()) {
					// System.out.println("<<<DELETE: " + row);

					writeDiff("DELETE", row);

				}

			} else if (delta.getType() == Delta.TYPE.CHANGE) {
				// get the largest from original & revised
				// int orig = delta.getOriginal().getLines().size();
				int rev = delta.getRevised().getLines().size();
				List<Integer> rMatches = new ArrayList<Integer>();

				String left = "";

				// totals
				int lCount = 0;
				// iterate over the left change objects (there may be less/=/more than the right
				// changes)
				for (Object leftObj : delta.getOriginal().getLines()) {
					left = (String) leftObj;
					int pos = this.ordinalIndexOf(left, "|", 4);
					String leftSignificant = left.substring(0, pos);
					String rightSignificant;

					int rCount = 0;
					boolean matched = false;
					for (Object rightObj : delta.getRevised().getLines()) {
						String right = (String) rightObj;
						pos = this.ordinalIndexOf(right, "|", 4);
						rightSignificant = right.substring(0, pos);
						// System.out.println(rCount + " right:" + rightSignificant);

						if (leftSignificant.equals(rightSignificant)) {
							// System.out.println("match.." + lCount + "==" + rCount);
							matches.put(Integer.toString(lCount), Integer.toString(rCount));
							// rMatches.add(new Integer(rCount));
							rMatches.add(Integer.valueOf(rCount));
							matched = true;
							break;
						} else {
							// not a match
						}
						rCount++;
					}
					// did it match
					if (matched) {
						// actual change
						// System.out.println("add a change here: " + lCount + "==" + rCount);
						utot++;
						// System.out.println("<><> from: " + left);
						// System.out.println("<><> to: " + delta.getRevised().getLines().get(rCount));
						writeDiff("CHANGE", left, (String) delta.getRevised().getLines().get(rCount));
					} else {
						// delete - not a change
						// System.out.println("add a delete delta " + lCount);
						dtot++;
						// System.out.println("<!<<DELETE: " + left);
						writeDiff("DELETE", left);
					}

					lCount++;
				}

				// if there are more objects on the right - we wouldn't have processed them
				// our ordering may be a little off - but it is insignificant
				// example: the 1st element is an insert and the 4th )ther other 2 are changes
				// - we would put the 1st after the 3rd (appending after existing matches)
				for (int rMatch = 0; rMatch < rev; rMatch++) {
					// check if matched...
					// if (rMatches.contains(new Integer(rMatch))) {
					if (rMatches.contains(Integer.valueOf(rMatch))) {
						// already matched
					} else {
						// System.out.println("adding un-matched rVal " + rMatch);
						// Delta newDelta = new Delta();
						itot++;
						// System.out.println("<!>>INSERT: " +
						// delta.getRevised().getLines().get(rMatch));
						writeDiff("INSERT", (String) delta.getRevised().getLines().get(rMatch));
					}
				}

				// reset the matches - for next iteration
				matches.clear();

			} // end - type of change (insert/delete/change)
		} // end for each delta

		System.out.println(
				"Total Changes: " + (dtot + itot + utot) + " delete:" + dtot + " insert:" + itot + " update:" + utot);
		System.out.println(messageTEXT.toString());
		// System.out.println("analyzer: d=" + dchunks + ":" + dtot);
		// System.out.println("analyzer: i=" + ichunks + ":" + itot);
		// System.out.println("analyzer: c=" + uchunks + ":" + utot);
		// System.out.println("analyzer: extra deletes=" + extraDeletes + " inserts:" +
		// extraInserts + " actualChanges:" + actChanges);

		return dtot + itot + utot;
	} // end analyzeChanges

	/**
	 * for simple diffs (insert or delete)
	 * 
	 * @param changeType
	 * @param row
	 */
	private void writeDiff(String changeType, String row) {
		// System.out.println("\t" + changeType.toLowerCase()+ ": " + row);
		messageTEXT.append("\t" + changeType.toLowerCase() + ": " + row + "\n");
		String colour = "green";
		if (changeType.equals("DELETE")) {
			colour = "red";
		}
		String parts[] = row.split("\\|", -1);
		messageHTMLx.append("<tr><td><font color=\"" + colour + "\">" + changeType + "</font></td>");
		for (String part : parts) {
			messageHTMLx.append("<td><font color=\"" + colour + "\">" + part + "</td></td>");
		}
		messageHTMLx.append("</tr>\n");
		writer.writeNext(ObjectArrays.concat(new String[] { changeType }, parts, String.class));
	}

	private void writeDiff(String changeType, String fromRow, String toRow) {
		messageTEXT.append("\t" + changeType.toLowerCase() + " from : " + fromRow + "\n");
		messageTEXT.append("\t" + changeType.toLowerCase() + "   to : " + toRow + "\n");

		String fromParts[] = fromRow.split("\\|", -1);
		String toParts[] = toRow.split("\\|", -1);

		messageHTMLx.append("<tr><td>" + changeType + " from" + "</td>");
		int partNum = 0;
		for (String part : fromParts) {
			String toPart = "";
			if (toParts.length >= partNum + 1) {
				toPart = toParts[partNum];
			}

			if (!part.equalsIgnoreCase(toPart)) {
				messageHTMLx.append("<td><font color=\"red\">" + part + "</font></td>");
			} else {
				messageHTMLx.append("<td>" + part + "</td>");
			}
			partNum++;
		}
		messageHTMLx.append("</tr>\n");
		messageHTMLx.append("<tr><td>" + changeType + " to" + "</td>");
		partNum = 0;
		for (String part : toParts) {
			// get the equivalent from part (which could be null)
			String fromPart = "";
			// System.out.println("checking: partNum=" + partNum + " in fromParts size=" +
			// fromParts.length + " " + fromParts.toString());
			// System.out.println("\t" + fromParts[partNum]);
			if (fromParts.length >= partNum + 1) {
				fromPart = fromParts[partNum];
			}
			// if (! part.equalsIgnoreCase(fromParts[partNum])) {
			if (!part.equalsIgnoreCase(fromPart)) {
				messageHTMLx.append("<td><font color=\"red\">" + part + "</font></td>");
			} else {
				messageHTMLx.append("<td>" + part + "</td>");
			}
			partNum++;
		}
		messageHTMLx.append("</tr>\n");
		// }

		writer.writeNext(ObjectArrays.concat(new String[] { changeType + " from" }, fromParts, String.class));
		writer.writeNext(ObjectArrays.concat(new String[] { changeType + " to" }, toParts, String.class));

	}

	CSVWriter writer = null;
	BufferedWriter bw;
	String diffFileName;

	/**
	 * initialize the file that will contain the results of the diff process
	 * 
	 * @param path  - folder to write the file
	 * @param left  - filename of the older/left file
	 * @param right - filename of the newer/right file
	 */
	public void setupCSVFile(String path, String left, String right) {
		// Path to the output report
		diffFileName = path + "/" + left.substring(0, left.length() - 4) + "_differences_"
				+ right.substring(0, right.length() - 4) + ".txt";
		System.out.println("\tinitializing file:" + diffFileName);
		try {
			// writer = new CSVWriter(new FileWriter(csv));
			// writer = new CSVWriter(new FileWriter(csv), CSVWriter.DEFAULT_SEPARATOR,
			// CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER,
			// CSVWriter.DEFAULT_LINE_END);
			bw = new BufferedWriter(new FileWriter(diffFileName));
			writer = new CSVWriter(bw, "\t".charAt(0), CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);

			if (this.includeAxonLinks) {
				writer.writeNext(new String[] { "action", "database", "schemaName", "table", "column", "type", "length",
						"scale" });
			} else {
				writer.writeNext(new String[] { "action", "database", "schemaName", "table", "column", "type", "length",
						"scale", "Axon Term Name, Axon Term Id" });
			}
			// bw.flush();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	/**
	 * close the diff file
	 */
	public void closeCSVFile() {
		try {
			// bw.flush();
			// bw.close();
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int ordinalIndexOf(String str, String substr, int n) {
		int pos = str.indexOf(substr);
		while (--n > 0 && pos != -1)
			pos = str.indexOf(substr, pos + 1);
		return pos;
	}
}