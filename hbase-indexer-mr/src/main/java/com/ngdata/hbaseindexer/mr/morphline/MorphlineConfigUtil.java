package com.ngdata.hbaseindexer.mr.morphline;

import com.typesafe.config.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Created by brunatm on 4.10.16.
 */
public class MorphlineConfigUtil {

	private Config config;
	private Map<String, BiFunction<Map<String, Object>, Object, Map<String, Object>>> commandModifiers = new LinkedHashMap<>();
	private Map<String, List<Map<String, Object>>> addCommands = new LinkedHashMap<>();

	public MorphlineConfigUtil() {
	}

	public MorphlineConfigUtil(Document document) {
		Config morphlineConfig = getDocMorphline(document);
		setConfig(morphlineConfig);
	}

	public MorphlineConfigUtil(Config config) {
		setConfig(config);
	}

	public void setConfig(Config config) {
		this.config = config;
	}

	public void addCommandModifier(String matchingPattern, BiFunction<Map<String, Object>, Object, Map<String, Object>> modifier) {
		commandModifiers.put(matchingPattern, modifier);
	}

	/**
	 * Add command to morphline after other command
	 * Command is added only once after first afterCommand appearance
	 *
	 * @param command
	 * @param afterCommand command name after which this command will be inserted
	 */
	public void addCommand(Map<String, Object> command, String afterCommand) {
		List<Map<String, Object>> commands;
		if (addCommands.containsKey(afterCommand)) {
			commands = addCommands.get(afterCommand);
		} else {
			commands = new LinkedList<>();
			addCommands.put(afterCommand, commands);
		}
		commands.add(command);
	}

	public Config modifyMorphline(Object parValue) {
		config = modifyMorphline(config, parValue);
		return config;
	}

	private Config modifyMorphline(Config morphline, Object parValue) {
		List<Map<String, Object>> commands = (List<Map<String, Object>>) morphline.getAnyRef("commands");
		List<Object> modifiedCommands = new ArrayList<>();
		commandIteration:
		for (Map<String, Object> command : commands) {
			String commandName = command.entrySet().iterator().next().getKey();
			// modify
			Set<BiFunction<Map<String, Object>, Object, Map<String, Object>>> modifiers = commandModifiers.entrySet().stream()
					.filter(entry -> commandName.matches(entry.getKey()))
					.map(entry -> entry.getValue())
					.collect(Collectors.toSet());
			for (BiFunction<Map<String, Object>, Object, Map<String, Object>> modifier : modifiers) {
				command = modifier.apply(command, parValue);
				if (command == null)
					continue commandIteration;
			}
			modifiedCommands.add(command);

			// add
			List<Map<String, Object>> commandAddCommands = addCommands.get(commandName);
			if (commandAddCommands != null) {
				modifiedCommands.addAll(commandAddCommands);
				addCommands.remove(commandName);  // add only once
			}
		}
		morphline = morphline.withValue("commands", ConfigValueFactory.fromIterable(modifiedCommands));
		return morphline;
	}

	/**
	 * Modify morphline
	 * Removes or modifies load commands from morphline and keeps only the mapping part.
	 * Add mapping commands.
	 *
	 * @return
	 */
	public Config modifyConfig(Object parValue) {
		ConfigList configList = config.getList("morphlines");
		Config morphline = ((ConfigObject) configList.get(0)).toConfig();
		Config modifiedMorphline = modifyMorphline(morphline, parValue);
		List<Object> modifiedConfigList = configList.unwrapped();
		modifiedConfigList.set(0, modifiedMorphline.root().unwrapped());
		return config.withValue("morphlines", ConfigValueFactory.fromIterable(modifiedConfigList));
	}

	private static Config getDocMorphline(Document document) {
		Element parentElement = document.getDocumentElement();
		List<Element> morphlineStrings = evalXPathAsElementList("param[@name='morphlineString']", parentElement);
		String morphlineString = morphlineStrings.size() > 0 ? getAttribute(morphlineStrings.get(0), "value", true) : null;
		return morphlineString != null ? ConfigFactory.parseString(morphlineString) : null;
	}

	public static void setDocMorphline(Document document, Config morphlineConfig) {
		String morphlineString = morphlineConfig.root().render(ConfigRenderOptions.concise().setJson(false).setFormatted(true));
		setDocMorphline(document, morphlineString);
	}

	public static void setDocMorphline(Document document, String morphlineString) {
		Element parentElement = document.getDocumentElement();
		List<Element> morphlineStrings = evalXPathAsElementList("param[@name='morphlineString']", parentElement);
		if (morphlineStrings.size() > 0) {
			morphlineStrings.get(0).setAttribute("value", morphlineString);
		} else {
			throw new IllegalStateException("No morphlineString parameter attribute found in document. Cannot set value.");
		}
	}

	private static List<Element> evalXPathAsElementList(String expression, Node node) {
		try {
			XPathExpression expr = XPathFactory.newInstance().newXPath().compile(expression);
			NodeList list = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
			List<Element> newList = new ArrayList<>(list.getLength());
			for (int i = 0; i < list.getLength(); i++) {
				newList.add((Element) list.item(i));
			}
			return newList;
		} catch (XPathExpressionException e) {
			throw new IllegalStateException("Error evaluating XPath expression '" + expression + "'.", e);
		}
	}

	private static String getAttribute(Element element, String name, boolean required) {
		if (!element.hasAttribute(name)) {
			if (required)
				throw new IllegalStateException("Missing attribute " + name + " on element " + element.getLocalName());
			else
				return null;
		}

		return element.getAttribute(name);
	}
}
