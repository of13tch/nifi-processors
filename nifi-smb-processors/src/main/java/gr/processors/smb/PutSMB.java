package gr.processors.smb;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import jcifs.UniAddress;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;
import jcifs.smb.SmbSession;
import org.apache.nifi.util.StopWatch;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"cifs", "smb", "put", "copy", "filesystem"})
@CapabilityDescription("Write FlowFile data to CIFS File System (SMB)")
@ReadsAttribute(attribute = "filename", description = "The name of the file written to SMB comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to SMB is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.smb.path", description = "The absolute path to the file on SMB share in this attribute.")
})

public class PutSMB extends AbstractProcessor {
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to SMB share are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be written to SMB share for some reason are transferred to this relationship")
            .build();


    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("Domain to connect to SMB share")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("Username")
            .description("User to connect to SMB share")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to authenticate to the SMB share.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor DOMAINCONTROLLER = new PropertyDescriptor.Builder()
            .name("Domain Controller")
            .description("DC to authenticate to")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext context) {
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(DIRECTORY);
        supDescriptors.add(CONFLICT_RESOLUTION);
        supDescriptors.add(USER);
        supDescriptors.add(PASSWORD);
        supDescriptors.add(DOMAIN);
        supDescriptors.add(DOMAINCONTROLLER);
        properties = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String smbPath = context.getProperty(DIRECTORY).getValue() + filename;
        final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        final String domainController = context.getProperty(DOMAINCONTROLLER).getValue();
        final String username = context.getProperty(USER).getValue();
        final String domain = context.getProperty(DOMAIN).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final ComponentLog logger = getLogger();
        final Path configuredRootDirPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());


        try {
            final UniAddress dc = UniAddress.getByName(domainController);
            final NtlmPasswordAuthentication credentials = new NtlmPasswordAuthentication(domain, username, password);
            SmbSession.logon(dc, credentials);
            SmbFile smbFile = new SmbFile(smbPath, credentials);


            final Path rootDirPath = configuredRootDirPath;
            final Path absolutePath = rootDirPath.resolve(flowFile.getAttribute((CoreAttributes.FILENAME.key())));

            FileInputStream in = new FileInputStream(absolutePath.toAbsolutePath().toString());
            IOUtils.copy(in, smbFile.getOutputStream());

            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Throwable t) {
            flowFile = session.penalize(flowFile);
            logger.error("Penalizing {} and transferring to failure due to {}", new Object[]{flowFile, t});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
