package com.hitachi.hci.full.text.match.pattern.stage;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup.Builder;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.BooleanDocumentFieldValue;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.hds.ensemble.sdk.model.StandardFields;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * 
 * @author Dave Huh
 * 
 * FullTextMatchPattern reads text from stream and matches a regex pattern.
 * 
 * Outputs boolean field if pattern match or not.
 *
 */
public class FullTextMatchPatternStage implements StagePlugin
{

	
	private static final String PLUGIN_NAME = "com.hitachi.hci.full.text.match.pattern.stage.FullTextMatchPatternStage";
	private static final String PLUGIN_DISPLAY_NAME = "Full Text Match Pattern Stage";
	private static final String PLUGIN_DESCRIPTION = "Reads full text stream and match terms";

	private final PluginConfig config;
	private final PluginCallback callback;
	
	//TODO test
	private String label_input;
	private String pattern_input;
	private Pattern pattern;

	static final String SPACE_CONSTANT = "%SPACE";
	
	private static String inputStreamName = StandardFields.CONTENT;

	
	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName(inputStreamName)
			.setValue("fullText")
			.setRequired(true)
			.setUserVisibleName("Stream")
			.setUserVisibleDescription("Name of the stream to read text from (e.g. HCI_text)");
	
	//TODO test
	public static final ConfigProperty.Builder PROPERTY_LABEL = new ConfigProperty.Builder()
			.setName("label")
			.setValue("")
			.setRequired(true)
			.setUserVisibleName("Label")
			.setUserVisibleDescription("Category");
	
	public static final ConfigProperty.Builder PROPERTY_PATTERN = new ConfigProperty.Builder()
			.setName("pattern")
			.setValue("")
			.setRequired(true)
			.setUserVisibleName("Pattern (regex)")
			.setUserVisibleDescription("Pattern to be matched in text");

	
	private static List<ConfigProperty.Builder> inputStreamGroupProperties = new ArrayList<>();
	private static List<ConfigProperty.Builder> pattern_group = new ArrayList<>();
	private static List<ConfigProperty.Builder> pattern_label_single_value = new ArrayList<>();
	
	public static final String INPUT_STREAM_GROUP_NAME = "Input text stream";
	
	public static final String PATTERN_MATCH_GROUP_NAME = "Pattern to match (regex)";
	public static final String PATTERN_MATCH_GROUP_DESCRIPTION = "Boolean label if match and Pattern (regex)";
	
	public static final String PATTERN_LABEL_GROUP_NAME = "Pattern and Label";
	public static final String PATTERN_LABEL_GROUP_DESCRIPTION = "Single entry for label and pattern";
	
	private static List<String> groupLabels = new ArrayList<>();
	
	static
	{
		groupLabels.add("Label (Category)");
		groupLabels.add("Pattern (regex)");
	}

	
	static
	{
		inputStreamGroupProperties.add(PROPERTY_INPUT_STREAM_NAME);
	}
	
	//TODO
	static
	{
		pattern_label_single_value.add(PROPERTY_LABEL);
		pattern_label_single_value.add(PROPERTY_PATTERN);
	}
	
	
	
	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_INPUT_STREAM = new ConfigPropertyGroup.Builder(INPUT_STREAM_GROUP_NAME,
			null)
			.setType(PropertyGroupType.DEFAULT)
			.setConfigProperties(inputStreamGroupProperties);
	
	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_PATTERN_LABEL = new ConfigPropertyGroup.Builder(PATTERN_MATCH_GROUP_NAME, PATTERN_MATCH_GROUP_DESCRIPTION)
			.setType(PropertyGroupType.KEY_VALUE_TABLE)
			.setOptions(groupLabels)
			.setConfigProperties(pattern_group);
	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_PATTERN_LABEL_SINGLE_ENTRY = new ConfigPropertyGroup.Builder(PATTERN_LABEL_GROUP_NAME, PATTERN_MATCH_GROUP_DESCRIPTION)
			.setType(PropertyGroupType.SINGLE_VALUE_TABLE)
			.setConfigProperties(pattern_label_single_value);
	
	//TODO test
	// Default config
	// This default configuration will be returned to callers of getDefaultConfig().
	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(PROPERTY_GROUP_INPUT_STREAM)
//			.addGroup(PROPERTY_GROUP_PATTERN_LABEL)
			.addGroup(PROPERTY_GROUP_PATTERN_LABEL_SINGLE_ENTRY)
			.build();
			
	
	// Default constructor for new unconfigured plugin instances (can obtain default
	// config)
	public FullTextMatchPatternStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}
	
	private Map<String, Pattern> labelPatternMap;
	
	public FullTextMatchPatternStage(PluginConfig pluginConfig, PluginCallback callback) throws ConfigurationException
	{
		this.config = pluginConfig;
		this.callback = callback;
		this.validateConfig(this.config);
		
		FullTextMatchPatternStage.inputStreamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(), "HCI_text");
		
		//TODO test
		//Pre-compile Label and Pattern
		this.label_input = this.config.getPropertyValue(PROPERTY_LABEL.getName());
		this.pattern_input = this.config.getPropertyValue(PROPERTY_PATTERN.getName());
		
		try {
			this.pattern = Pattern.compile(pattern_input);
		} catch (PatternSyntaxException ex) {
			throw new ConfigurationException("Invalid regex syntax in pattern configuration", ex);
		}
		
		//Label & Pattern Properties
//		ConfigPropertyGroup labelPatternGroup = config.getGroup(PATTERN_MATCH_GROUP_NAME);
//		List<ConfigProperty> labelPatternEntries = labelPatternGroup.getProperties();
//		labelPatternMap = new HashMap<String, Pattern>();
//		
//		try {
//			//to ensure key is unique
//			int i = 0;
//			// Pre-compile patterns for efficiency
//			for (ConfigProperty property : labelPatternEntries)
//			{
//				Pattern pattern = Pattern.compile(property.getValue());
//				String fieldNamelabel = property.getName() + "_" + i;
//				if (fieldNamelabel != null)
//				{
//					fieldNamelabel = fieldNamelabel.replaceAll(SPACE_CONSTANT, " ");
//				}
//				labelPatternMap.put(fieldNamelabel, pattern);
//				i++;
//			}
//		} catch (PatternSyntaxException ex) {
//			throw new ConfigurationException("Invalid regex syntax in pattern configuration", ex);
//		}
		

	}
	

	@Override
	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	@Override
	public String getDescription() {
		return PLUGIN_DESCRIPTION;
	}

	@Override
	public String getDisplayName() {
		return PLUGIN_DISPLAY_NAME;
	}

	@Override
	public String getName() {
		return PLUGIN_NAME;
	}

	@Override
	public String getSubCategory() {
		return "Custom";
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		return PluginSession.NOOP_INSTANCE;
	}

	@Override
	public void validateConfig(PluginConfig arg0) throws ConfigurationException {
		Config.validateConfig(getDefaultConfig(), config);
		Config.validateConfig((Config) this.getDefaultConfig(), (Config) config);
		if (config == null) {
			throw new ConfigurationException("No configuration for Full Text Match Pattern Stage");
		}
		
		// InputStream Property
		ConfigPropertyGroup inputStreamGroup = config.getGroup(INPUT_STREAM_GROUP_NAME);
		if (inputStreamGroup == null)
		{
			throw new ConfigurationException(
					"Missing configuration for group \"" + INPUT_STREAM_GROUP_NAME + "\"");
		}
		
		//TODO
		// InputStream Property
		ConfigPropertyGroup patternLabelGroup = config.getGroup(PATTERN_LABEL_GROUP_NAME);
		if (patternLabelGroup == null)
		{
			throw new ConfigurationException(
					"Missing configuration for group \"" + PATTERN_LABEL_GROUP_NAME + "\"");
		}
		
		
//		//TODO
//		// Label & Pattern Properties
//		ConfigPropertyGroup labelPatternGroup = config.getGroup(PATTERN_MATCH_GROUP_NAME);
//		if (labelPatternGroup == null)
//		{
//			throw new ConfigurationException(
//					"Missing configuration for group \"" + PATTERN_MATCH_GROUP_NAME + "\"");
//		}
//		
//		List<ConfigProperty> labelPatternEntries = labelPatternGroup.getProperties();
//		if (labelPatternEntries == null || labelPatternEntries.isEmpty())
//		{
//			throw new ConfigurationException(
//					"Missing entries (label and/or pattern) on required field \"" + PATTERN_MATCH_GROUP_NAME + "\"");
//		}
//		
//		for (ConfigProperty property : labelPatternEntries)
//		{
//			//fieldname label entry
//			if (property.getName() == null || property.getName().isEmpty())
//			{
//				throw new ConfigurationException(
//						"Missing label entry (required field");
//			}
//			
//			//pattern entry
//			if (property.getValue() == null || property.getValue().isEmpty())
//			{
//				throw new ConfigurationException(
//						"Missing pattern entry (required field");
//			}
//			
//			try {
//				Pattern.compile(property.getValue());
//			} catch (PatternSyntaxException ex) {
//				throw new ConfigurationException("Invalid syntax in pattern configuration ", ex);
//			}
//			
//		}
//		
	}

	@Override
	public FullTextMatchPatternStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new FullTextMatchPatternStage(config, callback);
	}

	@Override
	public StagePluginCategory getCategory() {
		return StagePluginCategory.OTHER;
	}

	@Override
	public Iterator<Document> process(PluginSession session, Document inputDocument)
			throws ConfigurationException, PluginOperationFailedException {
		
		boolean matched = false;
		
		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(inputDocument);
		
		InputStream inputStream = this.callback.openNamedStream(inputDocument, inputStreamName);

		//TODO
		String label = label_input;
					
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream)))
		{
			//Read lines of text stream
			String line;
			while ((line = br.readLine()) != null)
			{

				//TODO
				Matcher m = pattern.matcher(line);
				if (m.find()) {
					matched = true;
					
					String fieldName = "$matched_" + label;
					
					docBuilder.setMetadata(fieldName
							, BooleanDocumentFieldValue.builder().setBoolean(matched).build());
					
					break;
				}
				//TODO
//				for (Map.Entry<String, Pattern> entry : labelPatternMap.entrySet())
//				{
//					Pattern pattern = entry.getValue();
//					String label = entry.getKey();
//					
//					Matcher m = pattern.matcher(line);
//					if (m.find()) {
//						matched = true;
//						
//						String fieldName = "$matched_" + label;
//						
//						docBuilder.setMetadata(fieldName
//								, BooleanDocumentFieldValue.builder().setBoolean(matched).build());
//						
//						break;
//					}
//				}

			}
			
		br.close();
			
		} catch (IOException e) {
			throw new PluginOperationRuntimeException
			(
					new PluginOperationFailedException("Error processing text stream: " + e.getMessage())
			);
		}
		
		//
		// HCI Document Streams
		//
		return new StreamingDocumentIterator() {
			boolean sentAllDocuments = false;

			// Computes the next Document to return to the processing pipeline.
			// When there are no more documents to return, returns endOfDocuments().
			// This method can be used to consume an Iterator and build Documents
			// for each individual element as this streaming Iterator is consumed.
			@Override
			protected Document getNextDocument() {
				if (!sentAllDocuments) {
					sentAllDocuments = true;
					return docBuilder.build();
				}
				return endOfDocuments();
			}
		};
	}

}
